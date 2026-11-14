use crate::PreparedUpstream;
use anyhow::{bail, ensure, Result};
use http::{HeaderMap, HeaderName, Request, Uri};
use prost_config::Upstream;
use sha2::{Digest, Sha256};
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

pub(crate) struct Lease(Arc<AtomicU64>);
impl Lease {
    fn new(counter: &Arc<AtomicU64>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter.clone())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let before = self.0.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(before > 0);
    }
}
pub(crate) struct Selection {
    pub index: usize,
    pub lease: Option<Lease>,
}
#[derive(Debug)]
pub(crate) enum Rejection {
    Unavailable,
    Capacity,
}
impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "no healthy endpoints",
            Self::Capacity => "endpoint capacity exhausted",
        })
    }
}
impl std::error::Error for Rejection {}
#[derive(Clone)]
pub(crate) struct OriginalPath(pub http::uri::PathAndQuery);
pub(crate) struct Key<'a> {
    peer: IpAddr,
    uri: Option<&'a Uri>,
    original: Option<&'a http::uri::PathAndQuery>,
    headers: Option<&'a HeaderMap>,
}
impl<'a> Key<'a> {
    pub fn tcp(peer: IpAddr) -> Self {
        Self {
            peer,
            uri: None,
            original: None,
            headers: None,
        }
    }
    pub fn http<B: 'static>(
        peer: IpAddr,
        request: &'a Request<B>,
        original: Option<&'a http::uri::PathAndQuery>,
    ) -> Self {
        Self {
            peer,
            uri: Some(request.uri()),
            original: original.or_else(|| request.extensions().get::<OriginalPath>().map(|p| &p.0)),
            headers: Some(request.headers()),
        }
    }
}
enum HashKey {
    Peer,
    Uri,
    Original,
    Host,
    Header(HeaderName),
}
impl HashKey {
    fn compile(config: &Upstream) -> Result<Self> {
        Ok(match config.hash_key.as_deref().unwrap_or("$remote_addr") {
            "$remote_addr" => Self::Peer,
            "$uri" => Self::Uri,
            "$request_uri" => Self::Original,
            "$host" => Self::Host,
            s if s.starts_with("$http_") => {
                Self::Header(HeaderName::from_bytes(s[6..].replace('_', "-").as_bytes())?)
            }
            _ => {
                bail!("hash_key must be $remote_addr, $uri, $request_uri, $host or $http_<header>")
            }
        })
    }
    fn hash(&self, key: Key<'_>) -> Result<Option<u64>> {
        let mut hasher = Sha256::new();
        if matches!(self, Self::Peer) {
            match key.peer {
                IpAddr::V4(ip) => {
                    hasher.update([4]);
                    hasher.update(ip.octets());
                }
                IpAddr::V6(ip) => {
                    hasher.update([6]);
                    hasher.update(ip.octets());
                }
            }
        } else {
            let uri = key
                .uri
                .ok_or_else(|| anyhow::anyhow!("HTTP hash key on TCP session"))?;
            let headers = key.headers.unwrap();
            let bytes = match self {
                Self::Uri => uri.path().as_bytes(),
                Self::Original => key
                    .original
                    .or_else(|| uri.path_and_query())
                    .map_or(b"/".as_slice(), |p| p.as_str().as_bytes()),
                Self::Header(name) => headers.get(name).map_or(b"".as_slice(), |v| v.as_bytes()),
                Self::Host => host(uri, headers).as_bytes(),
                Self::Peer => unreachable!(),
            };
            if bytes.len() > 8192 {
                return Err(crate::FilterRejection {
                    status: 400,
                    retry_after: None,
                }
                .into());
            }
            if bytes.is_empty() {
                return Ok(None);
            }
            if matches!(self, Self::Host) && bytes.iter().any(u8::is_ascii_uppercase) {
                let mut lower = [0u8; 256];
                for chunk in bytes.chunks(lower.len()) {
                    for (target, source) in lower.iter_mut().zip(chunk) {
                        *target = source.to_ascii_lowercase();
                    }
                    hasher.update(&lower[..chunk.len()]);
                }
            } else {
                hasher.update(bytes);
            }
        }
        Ok(Some(u64::from_be_bytes(
            hasher.finalize()[..8].try_into().unwrap(),
        )))
    }
}
pub(crate) fn host<'a>(uri: &'a Uri, headers: &'a HeaderMap) -> &'a str {
    let host = uri
        .host()
        .or_else(|| {
            headers
                .get(http::header::HOST)
                .and_then(|v| v.to_str().ok())
        })
        .unwrap_or("");
    if let Some(h) = host.strip_prefix('[') {
        h.split(']').next().unwrap_or(h)
    } else {
        host.split(':').next().unwrap_or(host)
    }
}
struct Point {
    hash: u64,
    index: usize,
}
struct Ring {
    points: Vec<Point>,
}
impl Ring {
    fn new(config: &Upstream) -> Self {
        let replicas = config.hash_replicas.unwrap_or(160);
        let capacity = config
            .endpoints
            .iter()
            .map(|e| e.weight as usize * replicas as usize)
            .sum();
        let mut points = Vec::with_capacity(capacity);
        for (index, e) in config.endpoints.iter().enumerate() {
            let mut base = Sha256::new();
            base.update((e.id.len() as u32).to_be_bytes());
            base.update(e.id.as_bytes());
            base.update((e.address.len() as u32).to_be_bytes());
            base.update(e.address.as_bytes());
            for replica in 0..e.weight * replicas {
                let mut hash = base.clone();
                hash.update(replica.to_be_bytes());
                points.push(Point {
                    hash: u64::from_be_bytes(hash.finalize()[..8].try_into().unwrap()),
                    index,
                });
            }
        }
        points.sort_unstable_by(|a, b| {
            a.hash.cmp(&b.hash).then_with(|| {
                config.endpoints[a.index]
                    .id
                    .cmp(&config.endpoints[b.index].id)
            })
        });
        Self { points }
    }
    fn select(
        &self,
        upstream: &PreparedUpstream,
        hash: u64,
        allowed: Option<&[bool; 256]>,
    ) -> Result<usize> {
        let start = self.points.partition_point(|p| p.hash < hash);
        let primary = self.points[start % self.points.len()].index;
        let available = |i| allowed.map_or_else(|| eligible(upstream, i), |mask| mask[i]);
        if available(primary) {
            return Ok(primary);
        }
        ensure!(
            upstream
                .config
                .endpoints
                .iter()
                .enumerate()
                .any(|(i, _)| available(i)),
            Rejection::Unavailable
        );
        let mut seen = [false; 256];
        let mut visited = 0;
        for offset in 0..self.points.len() {
            let index = self.points[(start + offset) % self.points.len()].index;
            if seen[index] {
                continue;
            }
            seen[index] = true;
            visited += 1;
            if available(index) {
                return Ok(index);
            }
            if visited == upstream.config.endpoints.len() {
                break;
            }
        }
        Err(Rejection::Unavailable.into())
    }
}
struct LoadState {
    gate: Arc<Mutex<()>>,
    loads: Vec<Arc<AtomicU64>>,
}
enum Strategy {
    RoundRobin,
    Least,
    Hash(HashKey, Arc<Ring>),
}
pub(crate) struct Balancer {
    strategy: Strategy,
    counter: Arc<AtomicU64>,
    load: Option<LoadState>,
    has_backup: bool,
    plain_rr: bool,
}
impl Balancer {
    pub fn new(config: &Upstream, old: Option<&PreparedUpstream>) -> Result<Self> {
        let counter = old
            .map(|u| u.balancer.counter.clone())
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        let compatible =
            old.filter(|u| u.config.tls == config.tls && u.config.http2 == config.http2);
        let previous = compatible.and_then(|u| u.balancer.load.as_ref().map(|s| (u, s)));
        // Retain tracking after disabling limits: outstanding leases and later
        // re-enablement must see the same counters, even across algorithm changes.
        let load = (config.balance == "least_conn"
            || config.endpoints.iter().any(|e| e.max_active > 0)
            || previous.is_some())
        .then(|| LoadState {
            gate: previous
                .map(|(_, s)| s.gate.clone())
                .unwrap_or_else(|| Arc::new(Mutex::new(()))),
            loads: config
                .endpoints
                .iter()
                .map(|e| {
                    previous
                        .and_then(|(u, s)| {
                            u.config
                                .endpoints
                                .iter()
                                .position(|p| p.id == e.id && p.address == e.address)
                                .map(|i| s.loads[i].clone())
                        })
                        .unwrap_or_else(|| Arc::new(AtomicU64::new(0)))
                })
                .collect(),
        });
        let strategy = match config.balance.as_str() {
            "least_conn" => Strategy::Least,
            "consistent_hash" => {
                let key = HashKey::compile(config)?;
                let ring = old
                    .and_then(|u| match &u.balancer.strategy {
                        Strategy::Hash(_, ring)
                            if u.config.hash_replicas == config.hash_replicas
                                && u.config.endpoints.len() == config.endpoints.len()
                                && u.config.endpoints.iter().zip(&config.endpoints).all(
                                    |(a, b)| {
                                        a.id == b.id
                                            && a.address == b.address
                                            && a.weight == b.weight
                                    },
                                ) =>
                        {
                            Some(ring.clone())
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| Arc::new(Ring::new(config)));
                Strategy::Hash(key, ring)
            }
            "round_robin" => Strategy::RoundRobin,
            _ => bail!("unsupported balance algorithm"),
        };
        let has_backup = config.endpoints.iter().any(|e| e.backup);
        let plain_rr = matches!(strategy, Strategy::RoundRobin) && load.is_none() && !has_backup;
        Ok(Self {
            strategy,
            counter,
            load,
            has_backup,
            plain_rr,
        })
    }
    pub fn needs_original(&self) -> bool {
        matches!(self.strategy, Strategy::Hash(HashKey::Original, _))
    }
    pub fn active(&self, index: usize) -> Option<u64> {
        self.load
            .as_ref()
            .map(|s| s.loads[index].load(Ordering::Relaxed))
    }
    pub fn points(&self) -> usize {
        match &self.strategy {
            Strategy::Hash(_, r) => r.points.len(),
            _ => 0,
        }
    }
    #[inline]
    pub fn select_http<B: 'static>(
        &self,
        upstream: &PreparedUpstream,
        peer: IpAddr,
        request: &Request<B>,
        original: Option<&Uri>,
    ) -> Result<Selection> {
        if self.plain_rr {
            return Ok(Selection {
                index: self.round_robin(upstream)?,
                lease: None,
            });
        }
        self.select(
            upstream,
            Key::http(peer, request, original.and_then(Uri::path_and_query)),
        )
    }
    #[inline]
    pub fn select_tcp(&self, upstream: &PreparedUpstream, peer: IpAddr) -> Result<Selection> {
        if self.plain_rr {
            return Ok(Selection {
                index: self.round_robin(upstream)?,
                lease: None,
            });
        }
        self.select(upstream, Key::tcp(peer))
    }
    // Keep the SHA/ring/least-load state and stack buffers out of ordinary RR.
    #[inline(never)]
    pub fn select(&self, upstream: &PreparedUpstream, key: Key<'_>) -> Result<Selection> {
        if let Some(state) = &self.load {
            return self.select_tracked(upstream, state, key);
        }
        match &self.strategy {
            Strategy::RoundRobin => Ok(Selection {
                index: if self.has_backup {
                    self.round_robin_backup(upstream)?
                } else {
                    self.round_robin(upstream)?
                },
                lease: None,
            }),
            Strategy::Hash(source, ring) => {
                let hash = source.hash(key)?;
                let index = if let Some(hash) = hash {
                    ring.select(upstream, hash, None)?
                } else {
                    self.round_robin(upstream)?
                };
                Ok(Selection { index, lease: None })
            }
            Strategy::Least => unreachable!("least-load requires tracking"),
        }
    }
    #[inline(never)]
    fn select_tracked(
        &self,
        upstream: &PreparedUpstream,
        state: &LoadState,
        key: Key<'_>,
    ) -> Result<Selection> {
        // Hashing is outside the admission lock; only selection and reservation
        // are serialized across workers and compatible generations.
        let hash = match &self.strategy {
            Strategy::Hash(source, _) => source.hash(key)?,
            _ => None,
        };
        let _guard = state.gate.lock().unwrap_or_else(|e| e.into_inner());
        let mut allowed = [false; 256];
        let mut loads = [0; 256];
        let mut healthy = false;
        for (i, e) in upstream.config.endpoints.iter().enumerate() {
            loads[i] = state.loads[i].load(Ordering::Relaxed);
            let available = eligible(upstream, i);
            healthy |= available;
            allowed[i] = available && (e.max_active == 0 || loads[i] < e.max_active);
        }
        if !allowed.iter().any(|b| *b) {
            return Err(if healthy {
                Rejection::Capacity
            } else {
                Rejection::Unavailable
            }
            .into());
        }
        if self.has_backup {
            prefer_primary(upstream, &mut allowed);
        }
        let index = match &self.strategy {
            Strategy::RoundRobin => self.round_robin_allowed(upstream, &allowed)?,
            Strategy::Hash(_, ring) => match hash {
                Some(hash) => ring.select(upstream, hash, Some(&allowed))?,
                None => self.round_robin_allowed(upstream, &allowed)?,
            },
            Strategy::Least => {
                // Snapshot each load once; completions may race, but selections and
                // increments are serialized across workers and compatible generations.
                let mut best: Option<usize> = None;
                for (i, &load) in loads
                    .iter()
                    .enumerate()
                    .take(upstream.config.endpoints.len())
                {
                    if !allowed[i] {
                        continue;
                    }
                    if best.is_none_or(|b| {
                        ratio(
                            load,
                            upstream.config.endpoints[i].weight,
                            loads[b],
                            upstream.config.endpoints[b].weight,
                        )
                        .is_lt()
                    }) {
                        best = Some(i);
                    }
                }
                let best = best.ok_or_else(|| anyhow::anyhow!("no available endpoints"))?;
                let weight = upstream.config.endpoints[best].weight;
                let total: u64 = upstream
                    .config
                    .endpoints
                    .iter()
                    .enumerate()
                    .filter(|(i, e)| {
                        allowed[*i] && ratio(loads[*i], e.weight, loads[best], weight).is_eq()
                    })
                    .map(|(_, e)| u64::from(e.weight))
                    .sum();
                let mut ticket = self.counter.fetch_add(1, Ordering::Relaxed) % total;
                let mut selected = best;
                for (i, e) in upstream.config.endpoints.iter().enumerate() {
                    if !allowed[i] || !ratio(loads[i], e.weight, loads[best], weight).is_eq() {
                        continue;
                    }
                    if ticket < u64::from(e.weight) {
                        selected = i;
                        break;
                    }
                    ticket -= u64::from(e.weight);
                }
                selected
            }
        };
        Ok(Selection {
            index,
            lease: Some(Lease::new(&state.loads[index])),
        })
    }
    fn round_robin_allowed(&self, u: &PreparedUpstream, allowed: &[bool; 256]) -> Result<usize> {
        let total: u64 = u
            .config
            .endpoints
            .iter()
            .enumerate()
            .filter(|(i, _)| allowed[*i])
            .map(|(_, e)| u64::from(e.weight))
            .sum();
        ensure!(total > 0, Rejection::Unavailable);
        let mut ticket = self.counter.fetch_add(1, Ordering::Relaxed) % total;
        for (i, e) in u.config.endpoints.iter().enumerate() {
            if allowed[i] {
                if ticket < u64::from(e.weight) {
                    return Ok(i);
                }
                ticket -= u64::from(e.weight);
            }
        }
        unreachable!("stable weighted admission selection")
    }
    #[inline(never)]
    fn round_robin_backup(&self, u: &PreparedUpstream) -> Result<usize> {
        let mut allowed = [false; 256];
        for (i, value) in allowed
            .iter_mut()
            .enumerate()
            .take(u.config.endpoints.len())
        {
            *value = eligible(u, i);
        }
        prefer_primary(u, &mut allowed);
        self.round_robin_allowed(u, &allowed)
    }
    fn round_robin(&self, u: &PreparedUpstream) -> Result<usize> {
        if u.config.endpoints.len() == 1 {
            ensure!(eligible(u, 0), Rejection::Unavailable);
            return Ok(0);
        }
        let total: u64 = u
            .config
            .endpoints
            .iter()
            .enumerate()
            .filter(|(i, _)| eligible(u, *i))
            .map(|(_, e)| u64::from(e.weight))
            .sum();
        ensure!(total > 0, Rejection::Unavailable);
        let mut ticket = self.counter.fetch_add(1, Ordering::Relaxed) % total;
        for (i, e) in u.config.endpoints.iter().enumerate() {
            if eligible(u, i) {
                if ticket < u64::from(e.weight) {
                    return Ok(i);
                }
                ticket -= u64::from(e.weight);
            }
        }
        Err(Rejection::Unavailable.into())
    }
}
fn prefer_primary(u: &PreparedUpstream, allowed: &mut [bool; 256]) {
    if u.config
        .endpoints
        .iter()
        .enumerate()
        .any(|(i, e)| !e.backup && allowed[i])
    {
        for (i, e) in u.config.endpoints.iter().enumerate() {
            if e.backup {
                allowed[i] = false;
            }
        }
    }
}
fn eligible(u: &PreparedUpstream, i: usize) -> bool {
    u.config.endpoints[i].enabled && u.health[i].healthy.load(Ordering::Relaxed)
}
fn ratio(a: u64, aw: u32, b: u64, bw: u32) -> std::cmp::Ordering {
    (u128::from(a) * u128::from(bw)).cmp(&(u128::from(b) * u128::from(aw)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{prepare, Snapshot};
    use prost_config::{Config, Endpoint};
    fn config(algorithm: &str, weights: &[u32]) -> Config {
        let mut text = format!("schema_version=1\n[upstreams.main]\nbalance={algorithm:?}\n");
        for (i, weight) in weights.iter().enumerate() {
            text+=&format!("[[upstreams.main.endpoints]]\nid=\"node-{i}\"\naddress=\"127.0.0.1:{}\"\nweight={weight}\n",20000+i);
        }
        text+="[[listeners]]\nid=\"http\"\naddress=\"127.0.0.1:18080\"\nprotocol=\"http\"\nupstream=\"main\"\n";
        Config::parse(&text).unwrap()
    }
    fn snapshot(config: Config, old: Option<&Snapshot>) -> Result<Snapshot> {
        prepare(config, &prost_plugin::PluginRegistry::new()?, old, 1)
    }
    fn owner(u: &PreparedUpstream, key: u32) -> Result<String> {
        let selected = u.balancer.select(u, Key::tcp(IpAddr::V4(key.into())))?;
        Ok(u.config.endpoints[selected.index].id.clone())
    }
    #[tokio::test]
    async fn passive_policy_validation_mask_and_compatible_health_copy() -> Result<()> {
        let mut c = config("round_robin", &[1]);
        c.upstreams.get_mut("main").unwrap().passive_health = Some(prost_config::PassiveHealth {
            max_fails: 2,
            fail_timeout_ms: 10000,
            failure_statuses: vec![400, 503, 599],
        });
        let s = snapshot(c, None)?;
        let u = &s.upstreams["main"];
        for status in 100..=599 {
            assert_eq!(u.failed_status(status), [400, 503, 599].contains(&status));
        }
        u.health[0].passive_failure();
        let mut c = s.config.clone();
        c.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
        let next = snapshot(c, Some(&s))?;
        let n = &next.upstreams["main"];
        assert!(Arc::ptr_eq(&u.client, &n.client));
        assert!(!Arc::ptr_eq(&u.health[0], &n.health[0]));
        assert_eq!(n.health[0].passive_status().unwrap()["recent_failures"], 1);
        n.health[0].passive_failure();
        assert!(!n.health[0].healthy.load(Ordering::Relaxed));
        assert!(u.health[0].healthy.load(Ordering::Relaxed));
        let mut changed = next.config.clone();
        changed
            .upstreams
            .get_mut("main")
            .unwrap()
            .passive_health
            .as_mut()
            .unwrap()
            .max_fails = 1;
        let changed = snapshot(changed, Some(&next))?;
        let changed = &changed.upstreams["main"];
        assert!(Arc::ptr_eq(&n.client, &changed.client));
        assert!(changed.health[0].healthy.load(Ordering::Relaxed));
        n.health[0].observe(false, &n.config.health_check);
        let mut c = next.config.clone();
        c.upstreams
            .get_mut("main")
            .unwrap()
            .passive_health
            .as_mut()
            .unwrap()
            .max_fails = 0;
        let active_down = snapshot(c, Some(&next))?;
        assert!(!active_down.upstreams["main"].health[0]
            .healthy
            .load(Ordering::Relaxed));
        for (max_fails, timeout, statuses) in [
            (1001, 1000, vec![]),
            (1, 0, vec![]),
            (1, 3600001, vec![]),
            (1, 1000, vec![200]),
            (1, 1000, vec![600]),
            (1, 1000, vec![503, 503]),
            (1, 1000, (400..433).collect()),
        ] {
            let mut bad = s.config.clone();
            bad.upstreams.get_mut("main").unwrap().passive_health =
                Some(prost_config::PassiveHealth {
                    max_fails,
                    fail_timeout_ms: timeout,
                    failure_statuses: statuses,
                });
            assert!(bad.validate().is_err());
        }
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn endpoint_caps_are_atomic_for_every_algorithm() -> Result<()> {
        for algorithm in ["round_robin", "least_conn", "consistent_hash"] {
            let mut c = config(algorithm, &[1, 1]);
            let e = &mut c.upstreams.get_mut("main").unwrap().endpoints;
            e[0].max_active = 2;
            e[1].max_active = 3;
            let s = snapshot(c, None)?;
            let u = &s.upstreams["main"];
            let barrier = Arc::new(tokio::sync::Barrier::new(128));
            let tasks = (0..128)
                .map(|_| {
                    let barrier = barrier.clone();
                    let u = u.clone();
                    tokio::spawn(async move {
                        barrier.wait().await;
                        u.balancer.select_tcp(&u, "127.0.0.1".parse().unwrap())
                    })
                })
                .collect::<Vec<_>>();
            let results = futures_util::future::join_all(tasks).await;
            let mut leases = Vec::new();
            for result in results {
                if let Ok(selection) = result? {
                    leases.push(selection);
                }
            }
            assert_eq!(leases.len(), 5, "{algorithm}");
            assert_eq!(u.balancer.active(0), Some(2));
            assert_eq!(u.balancer.active(1), Some(3));
            assert!(u.balancer.select_tcp(u, "127.0.0.1".parse()?).is_err());
            let freed = leases.pop().unwrap().index;
            assert!(Arc::ptr_eq(
                &u.client,
                &snapshot(s.config.clone(), Some(&s))?.upstreams["main"].client
            ));
            // Releasing capacity must immediately admit another request, including
            // a key whose hash owner remains full.
            let next = u.balancer.select_tcp(u, "127.0.0.1".parse()?)?;
            assert_eq!(next.index, freed);
            drop(next);
            drop(leases);
            assert_eq!(u.balancer.active(0), Some(0));
            assert_eq!(u.balancer.active(1), Some(0));
        }
        // The single-endpoint path must also enforce admission atomically.
        let mut c = config("round_robin", &[1]);
        c.upstreams.get_mut("main").unwrap().endpoints[0].max_active = 1;
        let s = snapshot(c, None)?;
        let u = &s.upstreams["main"];
        let lease = u.balancer.select_tcp(u, "127.0.0.1".parse()?)?;
        assert!(u.balancer.select_tcp(u, "127.0.0.1".parse()?).is_err());
        drop(lease);
        assert!(u.balancer.select_tcp(u, "127.0.0.1".parse()?).is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn backup_tiers_weights_health_and_capacity() -> Result<()> {
        for algorithm in ["round_robin", "least_conn"] {
            let mut c = config(algorithm, &[1, 2, 1]);
            let endpoints = &mut c.upstreams.get_mut("main").unwrap().endpoints;
            endpoints[1].backup = true;
            endpoints[2].backup = true;
            let s = snapshot(c, None)?;
            let u = &s.upstreams["main"];
            for _ in 0..12 {
                assert_eq!(u.balancer.select_tcp(u, "127.0.0.1".parse()?)?.index, 0);
            }
            u.health[0].healthy.store(false, Ordering::Relaxed);
            let leases = (0..6)
                .map(|_| u.balancer.select_tcp(u, "127.0.0.1".parse().unwrap()))
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(leases.iter().filter(|s| s.index == 1).count(), 4);
            assert_eq!(leases.iter().filter(|s| s.index == 2).count(), 2);
            u.health[0].healthy.store(true, Ordering::Relaxed);
            assert_eq!(u.balancer.select_tcp(u, "127.0.0.1".parse()?)?.index, 0);
            drop(leases);
            let mut c = s.config.clone();
            for e in &mut c.upstreams.get_mut("main").unwrap().endpoints {
                e.max_active = 1;
            }
            let bounded = snapshot(c, Some(&s))?;
            let b = &bounded.upstreams["main"];
            let primary = b.balancer.select_tcp(b, "127.0.0.1".parse()?)?;
            assert_eq!(primary.index, 0);
            let backup1 = b.balancer.select_tcp(b, "127.0.0.1".parse()?)?;
            let backup2 = b.balancer.select_tcp(b, "127.0.0.1".parse()?)?;
            assert_ne!(backup1.index, backup2.index);
            assert_ne!(backup1.index, 0);
            assert_ne!(backup2.index, 0);
            assert!(b.balancer.select_tcp(b, "127.0.0.1".parse()?).is_err());
            drop(primary);
            assert_eq!(b.balancer.select_tcp(b, "127.0.0.1".parse()?)?.index, 0);
        }
        let mut invalid = config("consistent_hash", &[1, 1]);
        invalid.upstreams.get_mut("main").unwrap().endpoints[1].backup = true;
        assert!(invalid.validate().is_err());
        Ok(())
    }
    #[tokio::test]
    async fn admission_survives_algorithm_limit_and_pool_updates() -> Result<()> {
        let mut c = config("least_conn", &[1, 1]);
        c.upstreams.get_mut("main").unwrap().endpoints[0].max_active = 2;
        let s = snapshot(c, None)?;
        let u = &s.upstreams["main"];
        let lease = u.balancer.select_tcp(u, "127.0.0.1".parse()?)?;
        assert_eq!(lease.index, 0);
        let mut c = s.config.clone();
        let group = c.upstreams.get_mut("main").unwrap();
        group.balance = "consistent_hash".into();
        group.endpoints.reverse();
        group.endpoints[1].max_active = 1;
        let hashed = snapshot(c, Some(&s))?;
        let h = &hashed.upstreams["main"];
        assert!(Arc::ptr_eq(&u.client, &h.client));
        assert_eq!(h.balancer.active(1), Some(1));
        for _ in 0..16 {
            assert_eq!(h.balancer.select_tcp(h, "127.0.0.1".parse()?)?.index, 0);
        }
        let mut c = hashed.config.clone();
        let group = c.upstreams.get_mut("main").unwrap();
        group.balance = "round_robin".into();
        group.endpoints[1].max_active = 0;
        group.pool_max_idle_per_host = 32;
        let unlimited = snapshot(c, Some(&hashed))?;
        let r = &unlimited.upstreams["main"];
        assert!(!Arc::ptr_eq(&h.client, &r.client));
        assert_eq!(r.balancer.active(1), Some(1));
        let mut c = unlimited.config.clone();
        c.upstreams.get_mut("main").unwrap().endpoints[1].max_active = 1;
        let limited = snapshot(c, Some(&unlimited))?;
        assert_eq!(limited.upstreams["main"].balancer.active(1), Some(1));
        drop(lease);
        assert_eq!(limited.upstreams["main"].balancer.active(1), Some(0));
        Ok(())
    }
    #[tokio::test]
    async fn weighted_least_load_leases_and_concurrent_admission() -> Result<()> {
        let snapshot = snapshot(config("least_conn", &[2, 1]), None)?;
        let u = snapshot.upstreams["main"].clone();
        let mut leases = Vec::new();
        for _ in 0..6 {
            leases.push(u.balancer.select(&u, Key::tcp("127.0.0.1".parse()?))?);
        }
        assert_eq!(u.balancer.active(0), Some(4));
        assert_eq!(u.balancer.active(1), Some(2));
        drop(leases);
        assert_eq!(u.balancer.active(0), Some(0));
        assert_eq!(u.balancer.active(1), Some(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(64));
        let tasks = (0..64)
            .map(|_| {
                let u = u.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    u.balancer
                        .select(&u, Key::tcp("127.0.0.1".parse().unwrap()))
                })
            })
            .collect::<Vec<_>>();
        let leases = futures_util::future::join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let a = u.balancer.active(0).unwrap();
        let b = u.balancer.active(1).unwrap();
        assert_eq!(a + b, 64);
        assert!(a.abs_diff(2 * b) <= 2);
        drop(leases);
        assert_eq!(u.balancer.active(0), Some(0));
        assert_eq!(u.balancer.active(1), Some(0));
        Ok(())
    }
    #[tokio::test]
    async fn least_load_and_pool_survive_compatible_update() -> Result<()> {
        let old = snapshot(config("least_conn", &[1, 1]), None)?;
        let u = &old.upstreams["main"];
        let first = u.balancer.select(u, Key::tcp("127.0.0.1".parse()?))?;
        assert_eq!(first.index, 0);
        let mut c = old.config.clone();
        let group = c.upstreams.get_mut("main").unwrap();
        group.endpoints.reverse();
        group.endpoints[0].weight = 2;
        let next = snapshot(c, Some(&old))?;
        let n = &next.upstreams["main"];
        assert_eq!(n.balancer.active(1), Some(1));
        assert!(Arc::ptr_eq(&u.client, &n.client));
        let second = n.balancer.select(n, Key::tcp("127.0.0.1".parse()?))?;
        assert_eq!(second.index, 0);
        drop(first);
        assert_eq!(n.balancer.active(1), Some(0));
        drop(second);
        assert_eq!(n.balancer.active(0), Some(0));
        let mut changed = next.config.clone();
        changed.upstreams.get_mut("main").unwrap().endpoints[0].address = "127.0.0.1:29999".into();
        let changed = snapshot(changed, Some(&next))?;
        assert!(!Arc::ptr_eq(&n.client, &changed.upstreams["main"].client));
        Ok(())
    }
    #[tokio::test]
    async fn hash_stability_minimal_remap_weights_and_health() -> Result<()> {
        let old = snapshot(config("consistent_hash", &[1, 1, 1]), None)?;
        let u = &old.upstreams["main"];
        assert_eq!(u.balancer.points(), 480);
        let owners = (0..4000).map(|k| owner(u, k)).collect::<Result<Vec<_>>>()?;
        let mut c = old.config.clone();
        c.upstreams.get_mut("main").unwrap().endpoints.reverse();
        let reordered = snapshot(c, Some(&old))?;
        for (key, expected) in owners.iter().enumerate() {
            assert_eq!(&owner(&reordered.upstreams["main"], key as u32)?, expected);
        }
        let mut c = old.config.clone();
        c.upstreams
            .get_mut("main")
            .unwrap()
            .endpoints
            .push(Endpoint {
                id: "node-new".into(),
                address: "127.0.0.1:25000".into(),
                weight: 1,
                enabled: true,
                backup: false,
                max_active: 0,
            });
        let added = snapshot(c, Some(&old))?;
        let mut moved = 0;
        for (key, expected) in owners.iter().enumerate() {
            let actual = owner(&added.upstreams["main"], key as u32)?;
            if &actual != expected {
                assert_eq!(actual, "node-new");
                moved += 1;
            }
        }
        assert!((400..1800).contains(&moved));
        let mut c = old.config.clone();
        c.upstreams.get_mut("main").unwrap().endpoints[1].enabled = false;
        let disabled = snapshot(c, Some(&old))?;
        for (key, expected) in owners.iter().enumerate() {
            let actual = owner(&disabled.upstreams["main"], key as u32)?;
            assert_ne!(actual, "node-1");
            if expected != "node-1" {
                assert_eq!(&actual, expected);
            }
        }
        u.health[1].healthy.store(false, Ordering::Relaxed);
        for key in 0..4000 {
            assert_eq!(owner(u, key)?, owner(&disabled.upstreams["main"], key)?);
        }
        u.health[1].healthy.store(true, Ordering::Relaxed);
        for (key, expected) in owners.iter().enumerate() {
            assert_eq!(&owner(u, key as u32)?, expected);
        }
        let weighted = snapshot(config("consistent_hash", &[3, 1]), None)?;
        let count = (0..4000)
            .map(|k| owner(&weighted.upstreams["main"], k))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .filter(|s| s.as_str() == "node-0")
            .count();
        assert!(
            (2600..3400).contains(&count),
            "weighted owner count {count}"
        );
        Ok(())
    }
    #[tokio::test]
    async fn hash_key_semantics_empty_fallback_and_validation() -> Result<()> {
        let peer = "127.0.0.1".parse()?;
        let mut request = Request::builder()
            .uri("/rewritten?new=1")
            .header("host", "EXAMPLE.TEST:8443")
            .body(())?;
        request
            .extensions_mut()
            .insert(OriginalPath("/original?old=1".parse()?));
        let key = Key::http(peer, &request, None);
        assert_ne!(
            HashKey::Uri.hash(Key::http(peer, &request, None))?,
            HashKey::Original.hash(key)?
        );
        let original = Request::builder().uri("/original?old=1").body(())?;
        assert_eq!(
            HashKey::Original.hash(Key::http(peer, &request, None))?,
            HashKey::Original.hash(Key::http(peer, &original, None))?
        );
        let lower = Request::builder()
            .uri("/")
            .header("host", "example.test")
            .body(())?;
        assert_eq!(
            HashKey::Host.hash(Key::http(peer, &request, None))?,
            HashKey::Host.hash(Key::http(peer, &lower, None))?
        );
        let mut c = config("consistent_hash", &[2, 1]);
        c.upstreams.get_mut("main").unwrap().hash_key = Some("$http_x_session".into());
        let s = snapshot(c, None)?;
        let u = &s.upstreams["main"];
        let selected = (0..6)
            .map(|_| {
                u.balancer
                    .select(u, Key::http(peer, &request, None))
                    .map(|s| s.index)
            })
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(selected, [0, 0, 1, 0, 0, 1]);
        request
            .headers_mut()
            .insert("x-session", http::HeaderValue::from_str(&"a".repeat(8193))?);
        assert_eq!(
            u.balancer
                .select(u, Key::http(peer, &request, None))
                .err()
                .unwrap()
                .downcast_ref::<crate::FilterRejection>()
                .unwrap()
                .status,
            400
        );
        let mut c = s.config.clone();
        c.upstreams.get_mut("main").unwrap().hash_key = Some("$http_bad:name".into());
        assert!(snapshot(c, Some(&s)).is_err());
        let mut c = s.config.clone();
        c.upstreams.get_mut("main").unwrap().hash_replicas = Some(100000);
        assert!(c.validate().is_err());
        let mut c = s.config.clone();
        c.upstreams.get_mut("main").unwrap().endpoints[0].weight = 1000;
        assert!(c.validate().is_err());
        Ok(())
    }
}
