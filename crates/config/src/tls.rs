use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<TlsPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_ca: Option<PathBuf>,
    /// Default certificate, used when SNI is absent or has no explicit match.
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certificates: Vec<NamedCertificate>,
    #[serde(default)]
    pub reject_unknown_sni: bool,
    #[serde(default = "tls_buffer")]
    pub buffer_limit_bytes: usize,
}
fn tls_buffer() -> usize {
    64 * 1024
}
impl Default for ServerTls {
    fn default() -> Self {
        Self {
            policy: None,
            client_ca: None,
            certificate: PathBuf::new(),
            private_key: PathBuf::new(),
            certificates: Vec::new(),
            reject_unknown_sni: false,
            buffer_limit_bytes: tls_buffer(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamedCertificate {
    pub server_names: Vec<String>,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

impl ServerTls {
    pub fn validate(&self) -> Result<()> {
        if let Some(policy) = &self.policy {
            policy.validate()?;
        }
        ensure!(
            (4096..=1048576).contains(&self.buffer_limit_bytes),
            "TLS buffer_limit_bytes must be 4096..1048576"
        );
        ensure!(
            !self.certificate.as_os_str().is_empty() && !self.private_key.as_os_str().is_empty(),
            "default TLS certificate and private_key are required"
        );
        super::TlsSource::parse(&self.certificate)?;
        super::TlsSource::parse(&self.private_key)?;
        if let Some(ca) = &self.client_ca {
            super::TlsSource::parse(ca)?;
        }
        ensure!(
            self.certificates.len() <= 64,
            "at most 64 SNI certificate bundles per listener"
        );
        ensure!(
            !self.reject_unknown_sni || !self.certificates.is_empty(),
            "reject_unknown_sni requires explicit server_names"
        );
        let mut names = HashSet::new();
        for cert in &self.certificates {
            ensure!(
                !cert.server_names.is_empty(),
                "SNI certificate requires server_names"
            );
            ensure!(
                !cert.certificate.as_os_str().is_empty()
                    && !cert.private_key.as_os_str().is_empty(),
                "SNI certificate and private_key are required"
            );
            super::TlsSource::parse(&cert.certificate)?;
            super::TlsSource::parse(&cert.private_key)?;
            for name in &cert.server_names {
                ensure!(
                    names.insert(Self::normalize_name(name)?),
                    "duplicate SNI name {name}"
                );
                ensure!(names.len() <= 256, "at most 256 SNI names per listener");
            }
        }
        Ok(())
    }

    /// ASCII DNS names (use punycode for IDNs), with an optional full leftmost wildcard.
    pub fn normalize_name(name: &str) -> Result<String> {
        let name = name.strip_suffix('.').unwrap_or(name);
        let domain = name.strip_prefix("*.").unwrap_or(name);
        ensure!(
            !domain.is_empty()
                && domain.len() <= 253
                && domain.parse::<IpAddr>().is_err()
                && domain.split('.').all(|label| {
                    !label.is_empty()
                        && label.len() <= 63
                        && label.as_bytes()[0].is_ascii_alphanumeric()
                        && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                        && label
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                }),
            "invalid SNI DNS name {name}"
        );
        Ok(name.to_ascii_lowercase())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TlsMinimum {
    #[default]
    Tls12,
    Tls13,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsPolicy {
    pub min_version: TlsMinimum,
    pub session_resumption: bool,
    /// IANA/rustls suite names; empty retains the ring provider's safe defaults.
    pub cipher_suites: Vec<String>,
}
impl Default for TlsPolicy {
    fn default() -> Self {
        Self {
            min_version: TlsMinimum::Tls12,
            session_resumption: true,
            cipher_suites: Vec::new(),
        }
    }
}
impl TlsPolicy {
    pub fn validate(&self) -> Result<()> {
        const ALLOWED: &[&str] = &[
            "TLS13_AES_256_GCM_SHA384",
            "TLS13_AES_128_GCM_SHA256",
            "TLS13_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256",
        ];
        let unique: HashSet<_> = self.cipher_suites.iter().collect();
        ensure!(
            self.cipher_suites.len() <= ALLOWED.len()
                && unique.len() == self.cipher_suites.len()
                && self
                    .cipher_suites
                    .iter()
                    .all(|s| ALLOWED.contains(&s.as_str())),
            "unsupported or duplicate TLS cipher suite"
        );
        ensure!(
            self.min_version != TlsMinimum::Tls13
                || self.cipher_suites.is_empty()
                || self.cipher_suites.iter().any(|s| s.starts_with("TLS13_")),
            "TLS 1.3 requires a TLS13 cipher suite"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PassthroughSettings {
    pub client_hello_timeout_ms: u64,
    pub max_client_hello_bytes: usize,
    pub reject_unknown_sni: bool,
    pub server_names: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub routes: Vec<PassthroughRoute>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PassthroughRoute {
    pub server_names: Vec<String>,
    pub upstream: String,
}
impl Default for PassthroughSettings {
    fn default() -> Self {
        Self {
            client_hello_timeout_ms: 5000,
            max_client_hello_bytes: 65536,
            reject_unknown_sni: false,
            server_names: Vec::new(),
            routes: Vec::new(),
        }
    }
}
impl PassthroughSettings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=60000).contains(&self.client_hello_timeout_ms)
                && (4096..=65536).contains(&self.max_client_hello_bytes),
            "invalid passthrough ClientHello budgets"
        );
        ensure!(
            self.server_names.len() <= 256
                && self.routes.len() <= 64
                && (!self.reject_unknown_sni
                    || !self.server_names.is_empty()
                    || !self.routes.is_empty()),
            "strict passthrough SNI requires server_names or routes (max 256 names, 64 routes)"
        );
        let mut unique = HashSet::new();
        for name in &self.server_names {
            ensure!(
                unique.insert(ServerTls::normalize_name(name)?),
                "duplicate passthrough SNI name"
            );
        }
        let mut routed = HashSet::new();
        for route in &self.routes {
            super::valid_id(&route.upstream)?;
            ensure!(
                !route.server_names.is_empty(),
                "passthrough route requires server_names"
            );
            for name in &route.server_names {
                let normalized = ServerTls::normalize_name(name)?;
                ensure!(
                    routed.insert(normalized.clone()),
                    "duplicate passthrough route SNI name"
                );
                unique.insert(normalized);
                ensure!(unique.len() <= 256, "at most 256 passthrough SNI names");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn passthrough_routes_normalize_validate_and_roundtrip() -> Result<()> {
        let mut settings = PassthroughSettings {
            reject_unknown_sni: true,
            routes: vec![PassthroughRoute {
                server_names: vec!["API.Example.".into(), "*.example".into()],
                upstream: "app".into(),
            }],
            ..Default::default()
        };
        settings.validate()?;
        assert_eq!(
            toml::from_str::<PassthroughSettings>(&toml::to_string(&settings)?)?,
            settings
        );
        settings.routes.push(PassthroughRoute {
            server_names: vec!["api.EXAMPLE".into()],
            upstream: "other".into(),
        });
        assert!(settings.validate().is_err());
        settings.routes.pop();
        settings.routes[0].server_names.clear();
        assert!(settings.validate().is_err());
        settings.routes[0].server_names = (0..257).map(|i| format!("host{i}.test")).collect();
        assert!(settings.validate().is_err());
        settings.routes.clear();
        assert!(settings.validate().is_err());
        settings.reject_unknown_sni = false;
        settings.validate()?;
        Ok(())
    }
    #[test]
    fn tls_buffer_defaults_roundtrip_and_limits() -> Result<()> {
        let mut tls: ServerTls = toml::from_str("certificate='cert.pem'\nprivate_key='key.pem'")?;
        assert_eq!(tls.buffer_limit_bytes, 65536);
        tls.validate()?;
        for limit in [4096, 65536, 1048576] {
            tls.buffer_limit_bytes = limit;
            tls.validate()?;
            assert_eq!(toml::from_str::<ServerTls>(&toml::to_string(&tls)?)?, tls);
        }
        for limit in [0, 4095, 1048577] {
            tls.buffer_limit_bytes = limit;
            assert!(tls.validate().is_err());
        }
        Ok(())
    }
    #[test]
    fn sni_names_are_normalized_and_bounded() -> Result<()> {
        assert_eq!(
            ServerTls::normalize_name("API.Example.COM.")?,
            "api.example.com"
        );
        assert_eq!(ServerTls::normalize_name("*.Example.COM")?, "*.example.com");
        for name in [
            "",
            "*",
            "api.*.com",
            "api..com",
            ".example",
            "api.example..",
            "-api.example",
            "api-.example",
            "a_b.example",
            "127.0.0.1",
            "[::1]",
            "é.example",
        ] {
            assert!(ServerTls::normalize_name(name).is_err(), "{name}");
        }
        assert!(ServerTls::normalize_name(&format!("{}.com", "a".repeat(64))).is_err());
        let mut tls = ServerTls {
            certificate: "cert.pem".into(),
            private_key: "key.pem".into(),
            certificates: vec![NamedCertificate {
                server_names: vec!["api.example".into()],
                certificate: "cert.pem".into(),
                private_key: "key.pem".into(),
            }],
            reject_unknown_sni: true,
            ..Default::default()
        };
        tls.validate()?;
        tls.certificates[0].server_names.push("API.EXAMPLE.".into());
        assert!(tls.validate().is_err());
        tls.certificates.clear();
        assert!(tls.validate().is_err());
        Ok(())
    }
}
