use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, path::PathBuf};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    /// Default certificate, used when SNI is absent or has no explicit match.
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub certificates: Vec<NamedCertificate>,
    #[serde(default)]
    pub reject_unknown_sni: bool,
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
        ensure!(
            !self.certificate.as_os_str().is_empty() && !self.private_key.as_os_str().is_empty(),
            "default TLS certificate and private_key are required"
        );
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

#[cfg(test)]
mod tests {
    use super::*;
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
        };
        tls.validate()?;
        tls.certificates[0].server_names.push("API.EXAMPLE.".into());
        assert!(tls.validate().is_err());
        tls.certificates.clear();
        assert!(tls.validate().is_err());
        Ok(())
    }
}
