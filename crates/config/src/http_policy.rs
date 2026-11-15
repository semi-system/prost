use super::*;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HttpPolicy {
    pub compression: Option<Compression>,
    pub cache: Option<Cache>,
    pub retry: Option<Retry>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Compression {
    pub algorithms: Vec<String>,
    pub min_bytes: u64,
    pub level: u32,
    pub content_types: Vec<String>,
    pub max_concurrent: usize,
}
impl Default for Compression {
    fn default() -> Self {
        Self {
            algorithms: vec!["br".into(), "gzip".into()],
            min_bytes: 1024,
            level: 3,
            content_types: vec![
                "text/".into(),
                "application/json".into(),
                "application/javascript".into(),
                "application/xml".into(),
                "image/svg+xml".into(),
            ],
            max_concurrent: 16,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Cache {
    pub max_bytes: usize,
    pub max_entries: usize,
    pub max_object_bytes: usize,
    pub max_ttl_seconds: u64,
    pub revalidate: bool,
    pub revalidate_retention_seconds: u64,
    pub stale_if_error_seconds: u64,
    pub vary_headers: Vec<String>,
    pub stale_while_revalidate_seconds: u64,
    pub max_background_refreshes: usize,
    pub background_refresh_timeout_ms: u64,
}
impl Default for Cache {
    fn default() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            max_entries: 1024,
            max_object_bytes: 256 * 1024,
            max_ttl_seconds: 300,
            revalidate: false,
            revalidate_retention_seconds: 300,
            stale_if_error_seconds: 0,
            vary_headers: Vec::new(),
            stale_while_revalidate_seconds: 0,
            max_background_refreshes: 4,
            background_refresh_timeout_ms: 5000,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Retry {
    pub max_attempts: usize,
    pub statuses: Vec<u16>,
    pub timeout_ms: u64,
}
impl Default for Retry {
    fn default() -> Self {
        Self {
            max_attempts: 2,
            statuses: vec![502, 503, 504],
            timeout_ms: 5000,
        }
    }
}
impl HttpPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(c) = &self.compression {
            ensure!(
                !c.algorithms.is_empty()
                    && c.algorithms.len() <= 2
                    && c.algorithms
                        .iter()
                        .all(|s| matches!(s.as_str(), "br" | "gzip")),
                "invalid compression algorithms"
            );
            ensure!(
                c.level <= 6
                    && (1..=4096).contains(&c.max_concurrent)
                    && c.content_types.len() <= 64
                    && c.content_types
                        .iter()
                        .all(|s| !s.is_empty() && s.len() <= 128),
                "invalid compression budget"
            );
        }
        if let Some(c) = &self.cache {
            ensure!(
                (1..=1_073_741_824).contains(&c.max_bytes)
                    && (1..=100000).contains(&c.max_entries)
                    && (1..=16 * 1024 * 1024).contains(&c.max_object_bytes)
                    && c.max_object_bytes <= c.max_bytes
                    && (1..=86400).contains(&c.max_ttl_seconds)
                    && (1..=86400).contains(&c.revalidate_retention_seconds)
                    && c.stale_if_error_seconds <= 86400
                    && c.stale_while_revalidate_seconds <= 86400
                    && (1..=32).contains(&c.max_background_refreshes)
                    && (1..=60000).contains(&c.background_refresh_timeout_ms)
                    && c.vary_headers.len() <= 16
                    && c.vary_headers
                        .iter()
                        .all(|name| http::HeaderName::from_bytes(name.as_bytes()).is_ok()
                            && !["authorization", "cookie", "proxy-authorization"]
                                .contains(&name.to_ascii_lowercase().as_str())),
                "invalid cache budget"
            );
        }
        if let Some(r) = &self.retry {
            ensure!(
                (1..=8).contains(&r.max_attempts)
                    && (1..=86400000).contains(&r.timeout_ms)
                    && r.statuses.len() <= 16
                    && r.statuses.iter().all(|s| (500..=599).contains(s)),
                "invalid retry policy"
            );
        }
        Ok(())
    }
}
