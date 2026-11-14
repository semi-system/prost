use anyhow::{ensure, Context, Result};
use prost_config::ServerTls;
use rustls::{
    server::{ClientHello, ResolvesServerCert},
    sign::CertifiedKey,
    ServerConfig,
};
use std::{borrow::Cow, collections::HashMap, io::BufReader, path::Path, sync::Arc};

#[derive(Debug)]
struct SniResolver {
    default: Arc<CertifiedKey>,
    exact: HashMap<String, Arc<CertifiedKey>>,
    wildcard: HashMap<String, Arc<CertifiedKey>>,
    reject_unknown: bool,
}
impl SniResolver {
    fn choose(&self, name: Option<&str>) -> Option<Arc<CertifiedKey>> {
        if self.exact.is_empty() && self.wildcard.is_empty() {
            return (!self.reject_unknown).then(|| self.default.clone());
        }
        if let Some(name) = name {
            let name = name.strip_suffix('.').unwrap_or(name);
            let name = if name.bytes().any(|b| b.is_ascii_uppercase()) {
                Cow::Owned(name.to_ascii_lowercase())
            } else {
                Cow::Borrowed(name)
            };
            if let Some(cert) = self.exact.get(name.as_ref()) {
                return Some(cert.clone());
            }
            // A wildcard covers exactly one label, never the apex or deeper names.
            if let Some((_, suffix)) = name.split_once('.') {
                if let Some(cert) = self.wildcard.get(suffix) {
                    return Some(cert.clone());
                }
            }
        }
        (!self.reject_unknown).then(|| self.default.clone())
    }
}
impl ResolvesServerCert for SniResolver {
    fn resolve(&self, hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.choose(hello.server_name())
    }
}

fn load(certificate: &Path, private_key: &Path, bytes: &mut usize) -> Result<Arc<CertifiedKey>> {
    let cert = std::fs::read(certificate)
        .with_context(|| format!("read certificate {}", certificate.display()))?;
    let key = std::fs::read(private_key)
        .with_context(|| format!("read private key {}", private_key.display()))?;
    *bytes += cert.len() + key.len();
    ensure!(
        *bytes <= 16 * 1024 * 1024,
        "TLS PEM bundles exceed 16 MiB per listener"
    );
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key.as_slice()))?
        .context("missing private key")?;
    Ok(Arc::new(CertifiedKey::from_der(
        certs,
        key,
        &rustls::crypto::ring::default_provider(),
    )?))
}

pub fn server_tls(tls: &ServerTls, http: bool) -> Result<Arc<ServerConfig>> {
    tls.validate()?;
    let mut bytes = 0;
    let default = load(&tls.certificate, &tls.private_key, &mut bytes)?;
    let mut resolver = SniResolver {
        default,
        exact: HashMap::new(),
        wildcard: HashMap::new(),
        reject_unknown: tls.reject_unknown_sni,
    };
    // Reuse parsed certificate/signing keys when several bundles share the same files.
    let mut loaded = HashMap::new();
    loaded.insert(
        (&tls.certificate, &tls.private_key),
        resolver.default.clone(),
    );
    for bundle in &tls.certificates {
        let cert = if let Some(cert) = loaded.get(&(&bundle.certificate, &bundle.private_key)) {
            cert.clone()
        } else {
            let cert = load(&bundle.certificate, &bundle.private_key, &mut bytes)?;
            loaded.insert((&bundle.certificate, &bundle.private_key), cert.clone());
            cert
        };
        for name in &bundle.server_names {
            let name = ServerTls::normalize_name(name)?;
            if let Some(suffix) = name.strip_prefix("*.") {
                resolver.wildcard.insert(suffix.to_owned(), cert.clone());
            } else {
                resolver.exact.insert(name, cert.clone());
            }
        }
    }
    let builder =
        ServerConfig::builder_with_provider(crate::policy::provider(tls.policy.as_ref())?)
            .with_protocol_versions(crate::policy::versions(tls.policy.as_ref()))?;
    let builder = if let Some(path) = &tls.client_ca {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut BufReader::new(std::fs::read(path)?.as_slice())) {
            roots.add(cert?)?;
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build()?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    let mut config = builder.with_cert_resolver(Arc::new(resolver));
    if tls.policy.as_ref().is_some_and(|p| !p.session_resumption) {
        config.send_tls13_tickets = 0;
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    }
    if http {
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    }
    Ok(Arc::new(config))
}
