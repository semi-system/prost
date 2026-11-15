use anyhow::{ensure, Result};
use serde::Deserialize;
use std::{collections::HashSet, net::SocketAddr, path::PathBuf};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub directory_url: String,
    pub directory_ca: Option<PathBuf>,
    pub contact: String,
    pub terms_agreed: bool,
    pub state_dir: PathBuf,
    pub certificates_dir: PathBuf,
    pub admin_socket: PathBuf,
    pub challenge_address: SocketAddr,
    #[serde(default = "renew")]
    pub renew_before_seconds: u64,
    #[serde(default = "retry")]
    pub retry_seconds: u64,
    #[serde(default = "order_timeout")]
    pub order_timeout_seconds: u64,
    #[serde(default)]
    pub shared_certificate_group: bool,
    pub certificates: Vec<Certificate>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Certificate {
    pub id: String,
    pub domains: Vec<String>,
    pub listeners: Vec<String>,
    #[serde(default = "yes")]
    pub default_certificate: bool,
    #[serde(default)]
    pub add_sni_names: bool,
}
fn yes() -> bool {
    true
}
fn renew() -> u64 {
    30 * 86400
}
fn retry() -> u64 {
    300
}
fn order_timeout() -> u64 {
    180
}
impl Config {
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(text.len() <= 1024 * 1024, "agent config exceeds 1 MiB");
        let mut config: Self = toml::from_str(text)?;
        let url = url::Url::parse(&config.directory_url)?;
        ensure!(
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "ACME directory must be HTTPS without credentials/fragment"
        );
        ensure!(
            config.terms_agreed,
            "ACME terms must be explicitly accepted"
        );
        ensure!(
            config.contact.starts_with("mailto:")
                && config.contact.contains('@')
                && config.contact.len() <= 320
                && !config.contact.chars().any(char::is_control),
            "contact must be a mailto address"
        );
        ensure!(
            config.state_dir.is_absolute()
                && config.certificates_dir.is_absolute()
                && config.state_dir != config.certificates_dir
                && config.admin_socket.is_absolute(),
            "agent paths must be absolute and state/certificate roots distinct"
        );
        ensure!(
            config.challenge_address.port() > 0
                && (60..=86400).contains(&config.retry_seconds)
                && (30..=600).contains(&config.order_timeout_seconds)
                && (60..=90 * 86400).contains(&config.renew_before_seconds),
            "invalid agent budgets"
        );
        ensure!(
            !config.certificates.is_empty() && config.certificates.len() <= 64,
            "agent requires 1..64 certificate groups"
        );
        let (mut ids, mut domains, mut defaults) = (HashSet::new(), HashSet::new(), HashSet::new());
        for certificate in &mut config.certificates {
            ensure!(
                !certificate.id.is_empty()
                    && certificate.id.len() <= 63
                    && certificate
                        .id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
                    && ids.insert(certificate.id.clone()),
                "invalid/duplicate certificate id"
            );
            ensure!(
                !certificate.domains.is_empty()
                    && certificate.domains.len() <= 100
                    && !certificate.listeners.is_empty()
                    && certificate.listeners.len() <= 64,
                "invalid certificate domains/listeners"
            );
            for name in &mut certificate.domains {
                *name = prost_config::ServerTls::normalize_name(name)?;
                ensure!(
                    !name.contains('*')
                        && name.parse::<std::net::IpAddr>().is_err()
                        && domains.insert(name.clone()),
                    "HTTP-01 requires unique exact DNS names"
                );
            }
            let mut listeners = HashSet::new();
            for id in &certificate.listeners {
                ensure!(
                    !id.is_empty() && id.len() <= 63 && listeners.insert(id),
                    "invalid/duplicate target listener"
                );
                ensure!(
                    !certificate.default_certificate || defaults.insert(id.clone()),
                    "multiple default certificate groups target one listener"
                );
            }
        }
        Ok(config)
    }
}
