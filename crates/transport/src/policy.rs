use anyhow::Result;
use prost_config::{TlsMinimum, TlsPolicy};
use std::sync::Arc;

pub(crate) fn provider(policy: Option<&TlsPolicy>) -> Result<Arc<rustls::crypto::CryptoProvider>> {
    let mut provider = rustls::crypto::ring::default_provider();
    if let Some(policy) = policy {
        policy.validate()?;
        if !policy.cipher_suites.is_empty() {
            provider.cipher_suites = policy
                .cipher_suites
                .iter()
                .map(|name| {
                    provider
                        .cipher_suites
                        .iter()
                        .copied()
                        .find(|suite| format!("{:?}", suite.suite()) == *name)
                        .ok_or_else(|| anyhow::anyhow!("cipher suite {name} unavailable in ring"))
                })
                .collect::<Result<_>>()?;
        }
    }
    Ok(Arc::new(provider))
}
pub(crate) fn versions(
    policy: Option<&TlsPolicy>,
) -> &'static [&'static rustls::SupportedProtocolVersion] {
    static TLS13_ONLY: [&rustls::SupportedProtocolVersion; 1] = [&rustls::version::TLS13];
    static BOTH: [&rustls::SupportedProtocolVersion; 2] =
        [&rustls::version::TLS13, &rustls::version::TLS12];
    if policy.is_some_and(|p| p.min_version == TlsMinimum::Tls13) {
        &TLS13_ONLY
    } else {
        &BOTH
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_versions_and_cipher_families_exclude_legacy_tls_cbc_and_dhe() -> Result<()> {
        assert_eq!(
            versions(None).iter().map(|v| v.version).collect::<Vec<_>>(),
            vec![
                rustls::ProtocolVersion::TLSv1_3,
                rustls::ProtocolVersion::TLSv1_2
            ]
        );
        let provider = provider(None)?;
        assert!(!provider.cipher_suites.is_empty());
        for suite in &provider.cipher_suites {
            let name = format!("{:?}", suite.suite());
            assert!(
                name.starts_with("TLS13_") || name.starts_with("TLS_ECDHE_"),
                "{name}"
            );
            assert!(
                name.contains("GCM") || name.contains("CHACHA20_POLY1305"),
                "{name}"
            );
        }
        for name in [
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
            "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        ] {
            assert!(super::provider(Some(&TlsPolicy {
                cipher_suites: vec![name.into()],
                ..Default::default()
            }))
            .is_err());
        }
        Ok(())
    }
}
