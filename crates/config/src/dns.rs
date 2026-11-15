use super::*;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DnsPolicy {
    pub ttl_ms: u64,
    pub negative_ttl_ms: u64,
    pub timeout_ms: u64,
    pub max_hosts: usize,
    pub max_addresses: usize,
    pub authoritative_ttl: bool,
    pub nameservers: Vec<std::net::SocketAddr>,
}
impl Default for DnsPolicy {
    fn default() -> Self {
        Self {
            ttl_ms: 30000,
            negative_ttl_ms: 1000,
            timeout_ms: 2000,
            max_hosts: 1024,
            max_addresses: 32,
            authoritative_ttl: false,
            nameservers: Vec::new(),
        }
    }
}
impl DnsPolicy {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (100..=86400000).contains(&self.ttl_ms)
                && (100..=60000).contains(&self.negative_ttl_ms)
                && (1..=60000).contains(&self.timeout_ms)
                && (1..=1024).contains(&self.max_hosts)
                && (1..=64).contains(&self.max_addresses),
            "invalid DNS policy bounds"
        );
        ensure!(
            self.nameservers.len() <= 8 && self.nameservers.iter().all(|a| a.port() != 0),
            "invalid DNS nameservers"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validate_dns_and_cache_budgets_before_apply() {
        let text = "schema_version=1\nlisteners=[]\n[upstreams.web.dns]\nmax_hosts=1\n[[upstreams.web.endpoints]]\nid='a'\naddress='a.test:80'\n[[upstreams.web.endpoints]]\nid='b'\naddress='b.test:80'\n";
        assert!(Config::parse(text).is_err());
        assert!(Config::parse(&text.replace("b.test", "A.TEST.")).is_ok());
        assert!(Config::parse(&text.replace("b.test:80", "127.0.0.1:80")).is_ok());
        assert!(DnsPolicy {
            max_addresses: 0,
            ..Default::default()
        }
        .validate()
        .is_err());
        let invalid = HttpPolicy {
            cache: Some(Cache {
                stale_if_error_seconds: 86401,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }
}
