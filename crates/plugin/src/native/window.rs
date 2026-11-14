use super::{FilterDecision, FilterHook, NativeFilter};
use crate::rate_table::{canonical_ip, view, RateTable, TableKey, TableQuery};
use anyhow::{ensure, Result};
use http::HeaderMap;
use serde::Deserialize;
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    net::IpAddr,
    sync::Mutex,
    time::Instant,
};
fn keys() -> usize {
    4096
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    max_connections: u64,
    window_ms: u64,
    #[serde(default = "keys")]
    max_keys: usize,
    #[serde(default)]
    expire_ms: Option<u64>,
}
#[derive(Default)]
struct Window {
    start: u64,
    current: u64,
    previous: u64,
    last_seen: u64,
}
impl Window {
    fn estimate(&self, now: u64, period: u64) -> u64 {
        let now = now.max(self.start).max(self.last_seen);
        let passed = (now - self.start) / period;
        let remainder = (now - self.start) % period;
        let (current, previous) = match passed {
            0 => (self.current, self.previous),
            1 => (0, self.current),
            _ => (0, 0),
        };
        current.saturating_add(
            (previous as u128 * (period - remainder) as u128 / period as u128) as u64,
        )
    }
    fn record(&mut self, now: u64, period: u64) -> u64 {
        // Timestamps are sampled before taking the shard lock. A later worker
        // may rotate the period before an earlier sample obtains the lock.
        let now = now.max(self.start).max(self.last_seen);
        let passed = (now - self.start) / period;
        if passed > 0 {
            self.previous = if passed == 1 { self.current } else { 0 };
            self.current = 0;
            self.start += passed * period;
        }
        self.last_seen = now;
        self.current = self.current.saturating_add(1);
        let past =
            (self.previous as u128 * (period - (now - self.start)) as u128 / period as u128) as u64;
        self.current.saturating_add(past)
    }
}
struct Shard {
    entries: HashMap<IpAddr, Window>,
    capacity: usize,
    next_gc: u64,
}
pub(crate) struct ConnectionWindow {
    started: Instant,
    period: u64,
    limit: u64,
    expire: u64,
    hash: RandomState,
    shards: Vec<Mutex<Shard>>,
}
impl ConnectionWindow {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: Config = serde_json::from_value(value)?;
        ensure!(
            (1..=1000000).contains(&config.max_connections)
                && (1..=3600000).contains(&config.window_ms)
                && (1..=100000).contains(&config.max_keys),
            "invalid connection window limits"
        );
        let expire = config
            .expire_ms
            .unwrap_or((2 * config.window_ms).max(30000));
        ensure!(
            (2 * config.window_ms..=86400000).contains(&expire),
            "connection window expiry must cover two periods (max 1 day)"
        );
        let count = (config.max_keys / 64).clamp(1, 16);
        Ok(Self {
            started: Instant::now(),
            period: config.window_ms,
            limit: config.max_connections,
            expire,
            hash: RandomState::new(),
            shards: (0..count)
                .map(|i| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                        capacity: config.max_keys / count
                            + usize::from(i < config.max_keys % count),
                        next_gc: 0,
                    })
                })
                .collect(),
        })
    }
    fn admit_at(&self, ip: IpAddr, now: u64) -> bool {
        let ip = canonical_ip(ip);
        let index = self.hash.hash_one(ip) as usize % self.shards.len();
        let mut shard = self.shards[index].lock().unwrap();
        if now >= shard.next_gc {
            shard
                .entries
                .retain(|_, entry| now.saturating_sub(entry.last_seen) < self.expire);
            shard.next_gc = now.saturating_add(self.expire.min(1000));
        }
        if !shard.entries.contains_key(&ip) {
            if shard.entries.len() >= shard.capacity {
                return false;
            }
            shard.entries.insert(
                ip,
                Window {
                    start: now - now % self.period,
                    ..Default::default()
                },
            );
        }
        shard.entries.get_mut(&ip).unwrap().record(now, self.period) <= self.limit
    }
}
impl NativeFilter for ConnectionWindow {
    fn rate_table(&self) -> Option<&dyn RateTable> {
        Some(self)
    }
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == FilterHook::Connection
    }
    fn filter(
        &self,
        _: FilterHook,
        peer: IpAddr,
        _: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        Ok(
            if self.admit_at(
                peer,
                self.started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            ) {
                FilterDecision::Continue
            } else {
                FilterDecision::Reject {
                    status: 429,
                    retry_after: None,
                }
            },
        )
    }
}
impl RateTable for ConnectionWindow {
    fn inspect(&self, query: &TableQuery) -> Result<serde_json::Value> {
        query.validate()?;
        let key = query.key.as_ref().map(TableKey::ip).transpose()?;
        let now = self.started.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let mut entries = Vec::new();
        let (mut retained, mut capacity) = (0, 0);
        for shard in &self.shards {
            let shard = shard.lock().unwrap();
            retained += shard.entries.len();
            capacity += shard.capacity;
            let row = |ip: &IpAddr, entry: &Window| {
                serde_json::json!({
                "key":TableKey::text(ip.to_string()),"estimated_attempts":entry.estimate(now,self.period),
                "idle_ms":now.saturating_sub(entry.last_seen),"limit":self.limit,"window_ms":self.period})
            };
            if let Some(ip) = key {
                if let Some(entry) = shard.entries.get(&ip) {
                    entries.push(row(&ip, entry));
                }
            } else {
                for (ip, entry) in shard.entries.iter().take(query.limit - entries.len()) {
                    entries.push(row(ip, entry));
                }
            }
        }
        Ok(view(
            "weighted_window",
            "ip",
            capacity,
            retained,
            entries,
            key.is_some(),
        ))
    }
    fn clear(&self, key: Option<&TableKey>) -> Result<usize> {
        if let Some(key) = key {
            let ip = key.ip()?;
            let index = self.hash.hash_one(ip) as usize % self.shards.len();
            return Ok(usize::from(
                self.shards[index]
                    .lock()
                    .unwrap()
                    .entries
                    .remove(&ip)
                    .is_some(),
            ));
        }
        let mut cleared = 0;
        for shard in &self.shards {
            let mut shard = shard.lock().unwrap();
            cleared += shard.entries.len();
            shard.entries.clear();
        }
        Ok(cleared)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn table_inspection_is_read_only_and_clear_targets_one_canonical_ip() -> Result<()> {
        let limiter = ConnectionWindow::new(
            serde_json::json!({"max_connections":2,"window_ms":1000,"max_keys":4}),
        )?;
        let a = "127.0.0.1".parse()?;
        let b = "127.0.0.2".parse()?;
        assert!(limiter.admit_at(a, 1_000_000));
        assert!(limiter.admit_at(a, 1_000_000));
        assert!(!limiter.admit_at(a, 1_000_000));
        assert!(limiter.admit_at(b, 1_000_000));
        let query = TableQuery {
            key: Some(TableKey::text("::ffff:127.0.0.1")),
            limit: 1,
        };
        for _ in 0..2 {
            let report = limiter.inspect(&query)?;
            assert_eq!(report["entries"][0]["estimated_attempts"], 3);
            assert_eq!(report["entries"][0]["key"]["value"], "127.0.0.1");
        }
        assert_eq!(
            limiter.inspect(&TableQuery {
                limit: 1,
                ..Default::default()
            })?["truncated"],
            true
        );
        assert_eq!(limiter.clear(query.key.as_ref())?, 1);
        assert!(limiter.admit_at(a, 1_000_000));
        assert_eq!(limiter.clear(Some(&TableKey::text("127.0.0.9")))?, 0);
        assert!(limiter.clear(Some(&TableKey::text("bad-ip"))).is_err());
        assert_eq!(limiter.clear(None)?, 2);
        assert_eq!(
            limiter.inspect(&TableQuery::default())?["retained_entries"],
            0
        );
        let sample = Window {
            start: 0,
            current: 10,
            previous: 4,
            last_seen: 0,
        };
        assert_eq!(sample.estimate(50, 100), 12);
        assert_eq!(sample.estimate(100, 100), 10);
        assert_eq!(sample.estimate(150, 100), 5);
        assert_eq!(sample.estimate(200, 100), 0);
        Ok(())
    }
    #[test]
    fn weighted_window_counts_attempts_and_does_not_reset_at_boundary() -> Result<()> {
        let mut reordered = Window {
            start: 100,
            last_seen: 110,
            ..Default::default()
        };
        assert_eq!(reordered.record(99, 100), 1);
        assert_eq!(reordered.last_seen, 110);
        let limiter = ConnectionWindow::new(
            serde_json::json!({"max_connections":3,"window_ms":100,"max_keys":1,"expire_ms":200}),
        )?;
        let ip = "127.0.0.1".parse()?;
        for _ in 0..3 {
            assert!(limiter.admit_at(ip, 0));
        }
        assert!(!limiter.admit_at("::ffff:127.0.0.1".parse()?, 0));
        assert!(!limiter.admit_at(ip, 100));
        assert!(!limiter.admit_at(ip, 150));
        assert!(limiter.admit_at(ip, 200));
        assert!(!limiter.admit_at("127.0.0.2".parse()?, 200));
        assert!(limiter.admit_at("127.0.0.2".parse()?, 400));
        assert_eq!(limiter.shards[0].lock().unwrap().entries.len(), 1);
        Ok(())
    }
    #[test]
    fn concurrent_admission_and_configuration_are_bounded() -> Result<()> {
        let limiter = std::sync::Arc::new(ConnectionWindow::new(
            serde_json::json!({"max_connections":12,"window_ms":10000,"max_keys":1}),
        )?);
        let tasks: Vec<_> = (0..32)
            .map(|_| {
                let limiter = limiter.clone();
                std::thread::spawn(move || limiter.admit_at("127.0.0.1".parse().unwrap(), 0))
            })
            .collect();
        assert_eq!(
            tasks
                .into_iter()
                .filter_map(|t| t.join().ok())
                .filter(|v| *v)
                .count(),
            12
        );
        assert!(ConnectionWindow::new(
            serde_json::json!({"max_connections":1,"window_ms":1000,"expire_ms":1000})
        )
        .is_err());
        Ok(())
    }
}
