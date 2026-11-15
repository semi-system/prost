use anyhow::{ensure, Context, Result};
use http::{header, HeaderValue, Request, Uri};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheKeyInput {
    pub uri: String,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub accept_encoding: Vec<String>,
    #[serde(default)]
    pub vary_headers: std::collections::BTreeMap<String, Vec<String>>,
}
pub fn cache_key(input: &CacheKeyInput) -> Result<String> {
    ensure!(
        input.uri.len() <= 32768
            && input.host.as_ref().is_none_or(|s| s.len() <= 32768)
            && input.accept_encoding.len() <= 16
            && input.accept_encoding.iter().map(String::len).sum::<usize>() <= 32768
            && input.vary_headers.len() <= 16
            && input
                .vary_headers
                .iter()
                .all(|(name, values)| name.len() <= 256 && values.len() <= 16)
            && input
                .vary_headers
                .values()
                .map(|v| v.iter().map(String::len).sum::<usize>())
                .sum::<usize>()
                <= 32768,
        "cache key input exceeds bounds"
    );
    let uri: Uri = input.uri.parse().context("invalid cache key URI")?;
    ensure!(
        uri.path().starts_with('/')
            && ((uri.scheme().is_none() && uri.authority().is_none())
                || (matches!(uri.scheme_str(), Some("http" | "https"))
                    && uri.authority().is_some())),
        "cache key URI must be origin-form or absolute HTTP(S)"
    );
    let mut request = Request::builder().uri(uri).body(())?;
    if let Some(host) = &input.host {
        request
            .headers_mut()
            .insert(header::HOST, HeaderValue::from_bytes(host.as_bytes())?);
    }
    for value in &input.accept_encoding {
        request.headers_mut().append(
            header::ACCEPT_ENCODING,
            HeaderValue::from_bytes(value.as_bytes())?,
        );
    }
    for (name, values) in &input.vary_headers {
        let name = http::HeaderName::from_bytes(name.as_bytes())?;
        for value in values {
            request
                .headers_mut()
                .append(name.clone(), HeaderValue::from_str(value)?);
        }
    }
    let names: Vec<_> = input.vary_headers.keys().cloned().collect();
    Ok(encode_key(
        &crate::cache::Cache::key_with_vary(&request, &names)
            .context("request is not cache-key eligible")?,
    ))
}
pub(crate) fn encode_key(key: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut text = String::with_capacity(64);
    for byte in key {
        write!(text, "{byte:02x}").unwrap();
    }
    text
}
pub(crate) fn decode_key(text: &str) -> Result<[u8; 32]> {
    ensure!(text.len() == 64, "cache key must contain 64 hex digits");
    let mut key = [0; 32];
    for (out, pair) in key.iter_mut().zip(text.as_bytes().chunks_exact(2)) {
        let digit = |v: u8| {
            (v as char)
                .to_digit(16)
                .context("cache key must contain 64 hex digits")
        };
        *out = ((digit(pair[0])? << 4) | digit(pair[1])?) as u8;
    }
    Ok(key)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheQuery {
    pub key: Option<String>,
    pub limit: usize,
}
impl Default for CacheQuery {
    fn default() -> Self {
        Self {
            key: None,
            limit: 100,
        }
    }
}
impl CacheQuery {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=1024).contains(&self.limit),
            "cache entry limit must be 1..=1024"
        );
        if let Some(key) = &self.key {
            decode_key(key)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct CacheStatus {
    pub epoch: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub revalidations: u64,
    pub stale_hits: u64,
    pub purges: u64,
    pub purged_entries: u64,
    pub discarded_fills: u64,
    pub used_bytes: usize,
    pub entries: usize,
    pub active_captures: usize,
    pub background_refreshes: usize,
    pub max_bytes: usize,
    pub max_entries: usize,
    pub max_object_bytes: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct CacheEntry {
    pub key: String,
    pub body_bytes: usize,
    pub reservation_bytes: usize,
    pub age_seconds: u64,
    pub remaining_ttl_ms: u64,
    pub expired: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct CacheView {
    pub status: CacheStatus,
    pub entries: Vec<CacheEntry>,
    pub truncated: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct CachePurge {
    pub epoch: u64,
    pub removed_entries: usize,
    pub used_bytes_after: usize,
    pub active_captures: usize,
    pub invalidates_pre_purge_fills: bool,
}
