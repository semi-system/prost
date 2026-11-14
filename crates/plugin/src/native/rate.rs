use super::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use crate::rate_table::{canonical_ip, view, KeyEncoding, RateTable, TableKey, TableQuery};
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
    fn inspect<Q: Hash + Eq + ?Sized>(
        &self,
        key: Option<&Q>,
        limit: usize,
        row: impl Fn(&K, &Entry) -> serde_json::Value,
    ) -> (usize, usize, Vec<serde_json::Value>)
    where
        K: Borrow<Q>,
    {
        let (mut retained, mut capacity) = (0, 0);
        let mut entries = Vec::new();
        for shard in &self.shards {
            let shard = shard.lock().unwrap_or_else(|e| e.into_inner());
            retained += shard.entries.len();
            capacity += shard.capacity;
            if let Some(key) = key {
                if let Some((key, entry)) = shard.entries.get_key_value(key) {
                    entries.push(row(key, entry));
                }
            } else {
                for (key, entry) in shard.entries.iter().take(limit - entries.len()) {
                    entries.push(row(key, entry));
                }
            }
        }
        (capacity, retained, entries)
    }
    fn clear<Q: Hash + Eq + ?Sized>(&self, key: Option<&Q>) -> usize
    where
        K: Borrow<Q>,
    {
        if let Some(key) = key {
            let index = self.hash.hash_one(key) as usize % self.shards.len();
            let mut shard = self.shards[index].lock().unwrap_or_else(|e| e.into_inner());
            let Some(entry) = shard.entries.get(key) else {
                return 0;
            };
            entry.bucket.next.store(0, Ordering::Relaxed);
            // Keep in-flight users attached to the same bucket: removing their
            // key here would create independent old/new budgets after a clear.
            if Arc::strong_count(&entry.bucket) == 1 {
                shard.entries.remove(key);
            }
            return 1;
        }
        let mut cleared = 0;
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap_or_else(|e| e.into_inner());
            cleared += shard.entries.len();
            shard.entries.retain(|_, entry| {
                entry.bucket.next.store(0, Ordering::Relaxed);
                Arc::strong_count(&entry.bucket) > 1
            });
        }
        cleared
    }
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
    hook: FilterHook,
    started: Instant,
    global: Bucket,
    spacing: u64,
    allowance: u64,
    keyed: Option<Keyed>,
}
impl RateLimit {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        Self::build(value, FilterHook::RequestHeaders)
    }
    pub fn connections(mut value: serde_json::Value) -> Result<Self> {
        let object = value
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("connection_rate requires object config"))?;
        let rate = object
            .remove("connections_per_second")
            .ok_or_else(|| anyhow::anyhow!("connections_per_second required"))?;
        ensure!(
            !object.contains_key("requests_per_second"),
            "use connections_per_second"
        );
        object.insert("requests_per_second".into(), rate);
        let key = object
            .entry("key")
            .or_insert(serde_json::json!("$remote_addr"));
        ensure!(
            matches!(key.as_str(), Some("global" | "$remote_addr")),
            "connection rate key must be global or $remote_addr"
        );
        Self::build(value, FilterHook::Connection)
    }
    fn build(value: serde_json::Value, hook: FilterHook) -> Result<Self> {
        let c: Config = serde_json::from_value(value)?;
        ensure!(
            (1..=1_000_000).contains(&c.requests_per_second) && (1..=1_000_000).contains(&c.burst),
            "invalid rate limit"
        );
        ensure!(
            (1..=if hook == FilterHook::Connection {
                100000
            } else {
                65536
            })
                .contains(&c.max_keys)
                && (100..=86400000).contains(&c.idle_timeout_ms),
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
            hook,
            started: Instant::now(),
            global: Bucket::new(),
            spacing,
            allowance: spacing * (c.burst - 1),
            keyed,
        })
    }
    fn decision(&self, peer: IpAddr, headers: Option<&HeaderMap>, now: u64) -> FilterDecision {
        let peer = canonical_ip(peer);
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
    fn rate_table(&self) -> Option<&dyn RateTable> {
        Some(self)
    }
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == self.hook
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
impl RateTable for RateLimit {
    fn inspect(&self, query: &TableQuery) -> Result<serde_json::Value> {
        query.validate()?;
        let now = self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        let row = |key: TableKey, bucket: &Bucket, last_seen: Option<u64>| {
            let next = bucket.next.load(Ordering::Relaxed);
            serde_json::json!({"key":key,"debt_ms":next.saturating_sub(now).div_ceil(1_000_000),
                "retry_after_ms":next.saturating_sub(now.saturating_add(self.allowance)).div_ceil(1_000_000),
                "idle_ms":last_seen.map(|t|now.saturating_sub(t)/1_000_000)})
        };
        let (kind, capacity, retained, entries) = match &self.keyed {
            None => {
                validate_global(query.key.as_ref())?;
                (
                    "global",
                    1,
                    1,
                    vec![row(TableKey::text("global"), &self.global, None)],
                )
            }
            Some(Keyed::Peer(table)) => {
                let key = query.key.as_ref().map(TableKey::ip).transpose()?;
                let (c, n, entries) = table.inspect(key.as_ref(), query.limit, |ip, entry| {
                    row(
                        TableKey::text(ip.to_string()),
                        &entry.bucket,
                        Some(entry.last_seen),
                    )
                });
                ("ip", c, n, entries)
            }
            Some(Keyed::Header(_, table)) => {
                let key = query.key.as_ref().map(TableKey::bytes).transpose()?;
                let (c, n, entries) = table.inspect(key.as_deref(), query.limit, |key, entry| {
                    row(TableKey::header(key), &entry.bucket, Some(entry.last_seen))
                });
                ("header", c, n, entries)
            }
        };
        Ok(view(
            "gcra",
            kind,
            capacity,
            retained,
            entries,
            query.key.is_some(),
        ))
    }
    fn clear(&self, key: Option<&TableKey>) -> Result<usize> {
        Ok(match &self.keyed {
            None => {
                validate_global(key)?;
                self.global.next.store(0, Ordering::Relaxed);
                1
            }
            Some(Keyed::Peer(table)) => table.clear(key.map(TableKey::ip).transpose()?.as_ref()),
            Some(Keyed::Header(_, table)) => {
                table.clear(key.map(TableKey::bytes).transpose()?.as_deref())
            }
        })
    }
}
fn validate_global(key: Option<&TableKey>) -> Result<()> {
    ensure!(
        key.is_none_or(|k| matches!(k.encoding, KeyEncoding::Text) && k.value == "global"),
        "global rate table key must be text 'global'"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admin_clear_preserves_inflight_bucket_identity_and_shared_budget() {
        let table = Table::<IpAddr>::new(1, 100);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        let held = table.bucket(&ip, || ip, 0).unwrap();
        assert!(held.admit(0, 1_000_000_000, 0).is_none());
        for key in [Some(&ip), None] {
            assert_eq!(table.clear(key), 1);
            let current = table.bucket(&ip, || ip, 0).unwrap();
            assert!(Arc::ptr_eq(&held, &current));
            assert!(held.admit(0, 1_000_000_000, 0).is_none());
            assert!(current.admit(0, 1_000_000_000, 0).is_some());
        }
        drop(held);
        assert_eq!(table.clear(None::<&IpAddr>), 1);
        assert!(table.shards[0].lock().unwrap().entries.is_empty());
    }
    #[test]
    fn admin_queries_and_header_hex_round_trip_do_not_admit_requests() -> Result<()> {
        let rate = RateLimit::new(
            serde_json::json!({"requests_per_second":1,"burst":1,"key":"$http_x_key","max_keys":4}),
        )?;
        let ip = "127.0.0.1".parse()?;
        let mut headers = HeaderMap::new();
        headers.insert("x-key", http::HeaderValue::from_bytes(&[0x80, 0xff])?);
        assert!(matches!(
            rate.decision(ip, Some(&headers), 1_000_000_000),
            FilterDecision::Continue
        ));
        let query = TableQuery {
            key: Some(TableKey {
                value: "80ff".into(),
                encoding: KeyEncoding::Hex,
            }),
            limit: 1,
        };
        for _ in 0..2 {
            let report = rate.inspect(&query)?;
            assert_eq!(report["returned"], 1);
            assert_eq!(report["entries"][0]["key"]["value"], "80ff");
            assert!(report["entries"][0]["debt_ms"].as_u64().unwrap() > 0);
        }
        assert!(matches!(
            rate.decision(ip, Some(&headers), 1_000_000_000),
            FilterDecision::Reject { status: 429, .. }
        ));
        assert_eq!(rate.clear(query.key.as_ref())?, 1);
        assert!(matches!(
            rate.decision(ip, Some(&headers), 1_000_000_000),
            FilterDecision::Continue
        ));
        for value in ["0", "xx", &"aa".repeat(257)] {
            assert!(rate
                .clear(Some(&TableKey {
                    value: value.into(),
                    encoding: KeyEncoding::Hex
                }))
                .is_err());
        }
        assert_eq!(rate.inspect(&query)?["retained_entries"], 1);
        assert!(rate
            .inspect(&TableQuery {
                limit: 1025,
                ..Default::default()
            })
            .is_err());
        Ok(())
    }
    #[test]
    fn global_and_mapped_ip_admin_state_matches_admission() -> Result<()> {
        let rate =
            RateLimit::connections(serde_json::json!({"connections_per_second":1,"burst":1}))?;
        let ip = "127.0.0.1".parse()?;
        assert!(matches!(
            rate.decision(ip, None, 0),
            FilterDecision::Continue
        ));
        assert!(matches!(
            rate.decision("::ffff:127.0.0.1".parse()?, None, 0),
            FilterDecision::Reject { status: 429, .. }
        ));
        assert_eq!(rate.clear(Some(&TableKey::text("::ffff:127.0.0.1")))?, 1);
        assert!(matches!(
            rate.decision(ip, None, 0),
            FilterDecision::Continue
        ));
        let global = RateLimit::new(serde_json::json!({"requests_per_second":1,"burst":1}))?;
        assert!(matches!(
            global.decision(ip, None, 0),
            FilterDecision::Continue
        ));
        assert!(global.clear(Some(&TableKey::text("wrong"))).is_err());
        assert!(matches!(
            global.decision(ip, None, 0),
            FilterDecision::Reject { status: 429, .. }
        ));
        assert_eq!(global.clear(Some(&TableKey::text("global")))?, 1);
        assert!(matches!(
            global.decision(ip, None, 0),
            FilterDecision::Continue
        ));
        assert_eq!(
            global.inspect(&TableQuery::default())?["key_kind"],
            "global"
        );
        Ok(())
    }
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
