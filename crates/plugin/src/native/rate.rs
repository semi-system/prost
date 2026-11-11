use super::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use anyhow::{ensure, Result};
use http::{HeaderMap, HeaderName};
use serde::Deserialize;
use std::{
    borrow::Borrow,
    collections::{hash_map::RandomState, HashMap},
    hash::{BuildHasher, Hash},
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Instant,
};

fn global() -> String {
    "global".into()
}
fn max_keys() -> usize {
    4096
}
fn idle_ms() -> u64 {
    60000
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    requests_per_second: u64,
    burst: u64,
    #[serde(default = "global")]
    key: String,
    #[serde(default = "max_keys")]
    max_keys: usize,
    #[serde(default = "idle_ms")]
    idle_timeout_ms: u64,
}
struct Bucket {
    next: AtomicU64,
}
impl Bucket {
    fn new() -> Self {
        Self {
            next: AtomicU64::new(0),
        }
    }
    fn admit(&self, now: u64, spacing: u64, allowance: u64) -> Option<u64> {
        let mut previous = self.next.load(Ordering::Relaxed);
        loop {
            let available = now.saturating_add(allowance);
            if previous > available {
                return Some((previous - available).div_ceil(1_000_000_000).max(1));
            }
            let next = previous.max(now).saturating_add(spacing);
            match self.next.compare_exchange_weak(
                previous,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return None,
                Err(actual) => previous = actual,
            }
        }
    }
}
struct Entry {
    bucket: Arc<Bucket>,
    last_seen: u64,
}
struct Shard<K> {
    entries: HashMap<K, Entry>,
    capacity: usize,
    next_gc: u64,
}
struct Table<K> {
    shards: Vec<Mutex<Shard<K>>>,
    hash: RandomState,
    idle: u64,
}
impl<K: Hash + Eq> Table<K> {
    fn new(capacity: usize, idle_ms: u64) -> Self {
        // Small tables keep a single quota; larger tables amortize locking
        // without making a tiny shard reject most randomly distributed keys.
        let count = (capacity / 64).clamp(1, 16);
        Self {
            shards: (0..count)
                .map(|i| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                        capacity: capacity / count + usize::from(i < capacity % count),
                        next_gc: 0,
                    })
                })
                .collect(),
            hash: RandomState::new(),
            idle: idle_ms * 1_000_000,
        }
    }
    fn bucket<Q: Hash + Eq + ?Sized>(
        &self,
        key: &Q,
        own: impl FnOnce() -> K,
        now: u64,
    ) -> Option<Arc<Bucket>>
    where
        K: Borrow<Q>,
    {
        let index = self.hash.hash_one(key) as usize % self.shards.len();
        let mut shard = self.shards[index].lock().unwrap_or_else(|e| e.into_inner());
        if now >= shard.next_gc {
            // Rate debt and in-flight references survive eviction; a client cannot
            // reset a long burst allowance merely by waiting for the idle TTL.
            shard.entries.retain(|_, entry| {
                now.saturating_sub(entry.last_seen) < self.idle
                    || entry.bucket.next.load(Ordering::Relaxed) > now
                    || Arc::strong_count(&entry.bucket) > 1
            });
            shard.next_gc = now.saturating_add(self.idle.min(1_000_000_000));
        }
        if let Some(entry) = shard.entries.get_mut(key) {
            entry.last_seen = now;
            return Some(entry.bucket.clone());
        }
        if shard.entries.len() >= shard.capacity {
            return None;
        }
        let bucket = Arc::new(Bucket::new());
        shard.entries.insert(
            own(),
            Entry {
                bucket: bucket.clone(),
                last_seen: now,
            },
        );
        Some(bucket)
    }
}
enum Keyed {
    Peer(Table<IpAddr>),
    Header(HeaderName, Table<Box<[u8]>>),
}
pub(crate) struct RateLimit {
    started: Instant,
    global: Bucket,
    spacing: u64,
    allowance: u64,
    keyed: Option<Keyed>,
}
impl RateLimit {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let c: Config = serde_json::from_value(value)?;
        ensure!(
            (1..=1_000_000).contains(&c.requests_per_second) && (1..=1_000_000).contains(&c.burst),
            "invalid rate limit"
        );
        ensure!(
            (1..=65536).contains(&c.max_keys) && (100..=86400000).contains(&c.idle_timeout_ms),
            "invalid keyed rate limit bounds"
        );
        let keyed = match c.key.as_str() {
            "global" => None,
            "$remote_addr" => Some(Keyed::Peer(Table::new(c.max_keys, c.idle_timeout_ms))),
            s if s.starts_with("$http_") => Some(Keyed::Header(
                HeaderName::from_bytes(s[6..].replace('_', "-").as_bytes())?,
                Table::new(c.max_keys, c.idle_timeout_ms),
            )),
            _ => anyhow::bail!("rate key must be global, $remote_addr or $http_<header>"),
        };
        let spacing = 1_000_000_000u64.div_ceil(c.requests_per_second);
        Ok(Self {
            started: Instant::now(),
            global: Bucket::new(),
            spacing,
            allowance: spacing * (c.burst - 1),
            keyed,
        })
    }
    fn decision(&self, peer: IpAddr, headers: Option<&HeaderMap>, now: u64) -> FilterDecision {
        let shared = match &self.keyed {
            None => None,
            Some(Keyed::Peer(table)) => match table.bucket(&peer, || peer, now) {
                Some(b) => Some(b),
                None => {
                    return FilterDecision::Reject {
                        status: 503,
                        retry_after: Some(1),
                    }
                }
            },
            Some(Keyed::Header(name, table)) => {
                let key = headers
                    .and_then(|h| h.get(name))
                    .map_or(&[][..], |v| v.as_bytes());
                if key.len() > 256 {
                    return FilterDecision::Reject {
                        status: 400,
                        retry_after: None,
                    };
                }
                // Copy only when inserting. Retaining a sliced HeaderValue could
                // otherwise pin the entire incoming HTTP header buffer per key.
                match table.bucket(key, || key.into(), now) {
                    Some(b) => Some(b),
                    None => {
                        return FilterDecision::Reject {
                            status: 503,
                            retry_after: Some(1),
                        }
                    }
                }
            }
        };
        match shared
            .as_deref()
            .unwrap_or(&self.global)
            .admit(now, self.spacing, self.allowance)
        {
            None => FilterDecision::Continue,
            Some(seconds) => FilterDecision::Reject {
                status: 429,
                retry_after: Some(seconds),
            },
        }
    }
}
impl NativeFilter for RateLimit {
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == FilterHook::RequestHeaders
    }
    fn filter(
        &self,
        _: FilterHook,
        peer: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        Ok(self.decision(
            peer,
            headers.as_deref(),
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        ))
    }
    fn filter_with_context(
        &self,
        call: &FilterCall<'_>,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        self.filter(call.hook, call.peer, headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gcra_burst_refill_and_shared_budget() -> Result<()> {
        let rate = Arc::new(RateLimit::new(
            serde_json::json!({"requests_per_second":10,"burst":4}),
        )?);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let rate = rate.clone();
                std::thread::spawn(move || {
                    rate.global.admit(0, rate.spacing, rate.allowance).is_none()
                })
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .filter_map(|t| t.join().ok())
                .filter(|ok| *ok)
                .count(),
            4
        );
        assert!(rate
            .global
            .admit(99_999_999, rate.spacing, rate.allowance)
            .is_some());
        assert!(rate
            .global
            .admit(100_000_000, rate.spacing, rate.allowance)
            .is_none());
        Ok(())
    }
    #[test]
    fn keyed_capacity_idle_cleanup_and_rate_debt() -> Result<()> {
        let rate = RateLimit::new(
            serde_json::json!({"requests_per_second":1,"burst":1,"key":"$remote_addr","max_keys":1,"idle_timeout_ms":100}),
        )?;
        let a = "127.0.0.1".parse()?;
        let b = "127.0.0.2".parse()?;
        assert!(matches!(
            rate.decision(a, None, 0),
            FilterDecision::Continue
        ));
        assert!(matches!(
            rate.decision(a, None, 1),
            FilterDecision::Reject { status: 429, .. }
        ));
        assert!(matches!(
            rate.decision(b, None, 200_000_000),
            FilterDecision::Reject { status: 503, .. }
        ));
        // TTL elapsed, but the old key's rate debt has not: no eviction bypass.
        assert!(matches!(
            rate.decision(b, None, 1_200_000_000),
            FilterDecision::Continue
        ));
        assert!(matches!(
            rate.decision(a, None, 1_200_000_001),
            FilterDecision::Reject { status: 503, .. }
        ));
        Ok(())
    }
    #[test]
    fn header_keys_are_independent_and_missing_key_shares_budget() -> Result<()> {
        let rate = RateLimit::new(
            serde_json::json!({"requests_per_second":1,"burst":1,"key":"$http_x_api_key","max_keys":64}),
        )?;
        let peer = "127.0.0.1".parse()?;
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", http::HeaderValue::from_static("alice"));
        assert!(matches!(
            rate.decision(peer, Some(&headers), 0),
            FilterDecision::Continue
        ));
        headers.insert("x-api-key", http::HeaderValue::from_static("bob"));
        assert!(matches!(
            rate.decision(peer, Some(&headers), 0),
            FilterDecision::Continue
        ));
        headers.insert("x-api-key", http::HeaderValue::from_static("alice"));
        assert!(matches!(
            rate.decision(peer, Some(&headers), 0),
            FilterDecision::Reject { status: 429, .. }
        ));
        assert!(matches!(
            rate.decision(peer, None, 0),
            FilterDecision::Continue
        ));
        assert!(matches!(
            rate.decision(peer, None, 0),
            FilterDecision::Reject { status: 429, .. }
        ));
        headers.insert("x-api-key", http::HeaderValue::from_str(&"x".repeat(257))?);
        assert!(matches!(
            rate.decision(peer, Some(&headers), 0),
            FilterDecision::Reject { status: 400, .. }
        ));
        Ok(())
    }
}
