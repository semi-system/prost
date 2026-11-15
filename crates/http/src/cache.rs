use crate::cache_control::{
    decode_key, encode_key, CacheEntry, CachePurge, CacheQuery, CacheStatus, CacheView,
};
use crate::{Body, Error};
use anyhow::Context as _;
use bytes::{Bytes, BytesMut};
use http::{header, HeaderMap, Request, Response, Version};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime},
};

type Key = [u8; 32];
pub(crate) struct Cache {
    used: Arc<AtomicUsize>,
    config: prost_config::Cache,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    epoch: u64,
    purges: u64,
    purged_entries: u64,
    discarded_fills: u64,
    entries: HashMap<Key, Entry>,
    order: VecDeque<(u64, Key)>,
    sequence: u64,
    captures: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    revalidations: u64,
    stale_hits: u64,
    refreshing: HashSet<Key>,
}
#[derive(Clone)]
struct Entry {
    id: u64,
    headers: HeaderMap,
    version: Version,
    data: Bytes,
    created: Instant,
    expires: Instant,
    revalidate_until: Instant,
    retain_until: Instant,
    stale_until: Instant,
    age: u64,
    reservation: Arc<Reservation>,
}
pub struct Hit {
    pub headers: HeaderMap,
    pub version: Version,
    pub data: Bytes,
}
#[derive(Clone)]
pub(crate) struct Candidate {
    key: Key,
    epoch: u64,
    entry: Entry,
}
impl Candidate {
    pub fn condition(&self, headers: &mut HeaderMap) {
        if let Some(value) = self.entry.headers.get(header::ETAG) {
            headers.insert(header::IF_NONE_MATCH, value.clone());
        } else if let Some(value) = self.entry.headers.get(header::LAST_MODIFIED) {
            headers.insert(header::IF_MODIFIED_SINCE, value.clone());
        }
    }
}
fn hit(entry: &Entry) -> Hit {
    let mut headers = entry.headers.clone();
    headers.insert(
        header::AGE,
        http::HeaderValue::from_str(
            &entry
                .age
                .saturating_add(entry.created.elapsed().as_secs())
                .to_string(),
        )
        .unwrap(),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        http::HeaderValue::from_str(&entry.data.len().to_string()).unwrap(),
    );
    Hit {
        headers,
        version: entry.version,
        data: Bytes::from_owner(HitOwner {
            data: entry.data.clone(),
            _reservation: entry.reservation.clone(),
        }),
    }
}
struct HitOwner {
    data: Bytes,
    _reservation: Arc<Reservation>,
}
impl AsRef<[u8]> for HitOwner {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
pub(crate) struct Reservation {
    used: Arc<AtomicUsize>,
    cost: usize,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.cost, Ordering::Relaxed);
    }
}
impl Cache {
    pub fn variant_key<B>(&self, request: &Request<B>) -> Option<Key> {
        Self::key_with_vary(request, &self.config.vary_headers)
    }
    pub fn key_with_vary<B>(request: &Request<B>, names: &[String]) -> Option<Key> {
        let base = Self::key(request)?;
        if names.is_empty() {
            return Some(base);
        }
        let mut hash = Sha256::new();
        hash.update(base);
        let mut names: Vec<_> = names.iter().map(|name| name.to_ascii_lowercase()).collect();
        names.sort();
        names.dedup();
        for name in names {
            hash.update((name.len() as u64).to_be_bytes());
            hash.update(name.as_bytes());
            hash.update(
                (request.headers().get_all(name.as_str()).iter().count() as u64).to_be_bytes(),
            );
            for value in request.headers().get_all(name.as_str()) {
                hash.update((value.as_bytes().len() as u64).to_be_bytes());
                hash.update(value.as_bytes());
            }
        }
        Some(hash.finalize().into())
    }
    pub fn swr(self: &Arc<Self>, candidate: &Candidate) -> Option<(Hit, Option<Refresh>)> {
        let mut state = self.state.lock().unwrap();
        if state.epoch != candidate.epoch
            || state
                .entries
                .get(&candidate.key)
                .is_none_or(|entry| entry.id != candidate.entry.id)
            || candidate.entry.expires
                + Duration::from_secs(swr_budget(&self.config, &candidate.entry.headers))
                <= Instant::now()
        {
            return None;
        }
        state.hits += 1;
        state.stale_hits += 1;
        let refresh = if state.refreshing.len() < self.config.max_background_refreshes
            && state.refreshing.insert(candidate.key)
        {
            Some(Refresh {
                cache: self.clone(),
                key: candidate.key,
            })
        } else {
            None
        };
        Some((hit(&candidate.entry), refresh))
    }
    pub fn refresh_timeout(&self) -> Duration {
        Duration::from_millis(self.config.background_refresh_timeout_ms)
    }
    pub fn object_limit(&self) -> usize {
        self.config.max_object_bytes
    }
    pub fn new(config: prost_config::Cache) -> Arc<Self> {
        Arc::new(Self {
            used: Arc::new(AtomicUsize::new(0)),
            config,
            state: Mutex::new(State::default()),
        })
    }
    pub fn key<B>(request: &Request<B>) -> Option<Key> {
        if request.method() != http::Method::GET
            || [
                header::AUTHORIZATION,
                header::COOKIE,
                header::RANGE,
                header::IF_MATCH,
                header::IF_NONE_MATCH,
                header::IF_MODIFIED_SINCE,
                header::IF_UNMODIFIED_SINCE,
                header::EXPECT,
                header::UPGRADE,
            ]
            .iter()
            .any(|h| request.headers().contains_key(h))
        {
            return None;
        }
        if request.headers().contains_key(header::PRAGMA)
            || request
                .headers()
                .get_all(header::CACHE_CONTROL)
                .iter()
                .any(|h| {
                    h.to_str().map_or(true, |s| {
                        s.split(',').any(|s| {
                            matches!(
                                s.trim()
                                    .split('=')
                                    .next()
                                    .unwrap_or("")
                                    .to_ascii_lowercase()
                                    .as_str(),
                                "no-cache" | "no-store" | "max-age" | "min-fresh" | "max-stale"
                            )
                        })
                    })
                })
        {
            return None;
        }
        let mut hash = Sha256::new();
        for part in [
            request.uri().to_string().as_bytes(),
            request
                .headers()
                .get(header::HOST)
                .map_or(&[][..], |h| h.as_bytes()),
        ] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part);
        }
        for h in request.headers().get_all(header::ACCEPT_ENCODING) {
            hash.update((h.as_bytes().len() as u64).to_be_bytes());
            hash.update(h.as_bytes());
        }
        Some(hash.finalize().into())
    }
    pub fn lookup(&self, key: &Key) -> (Option<Hit>, u64) {
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .get(key)
            .is_some_and(|e| e.retain_until <= Instant::now())
        {
            state.entries.remove(key);
        }
        let hit = state
            .entries
            .get(key)
            .filter(|e| e.expires > Instant::now())
            .map(hit);
        if hit.is_some() {
            state.hits += 1;
        } else {
            state.misses += 1;
        }
        (hit, state.epoch)
    }
    pub fn candidate(&self, key: &Key, epoch: u64) -> Option<Candidate> {
        let state = self.state.lock().unwrap();
        let entry = state.entries.get(key)?;
        (state.epoch == epoch
            && entry.expires <= Instant::now()
            && entry.retain_until > Instant::now())
        .then(|| Candidate {
            key: *key,
            epoch,
            entry: entry.clone(),
        })
    }
    pub fn can_revalidate(&self, candidate: &Candidate) -> bool {
        self.config.revalidate
            && candidate.entry.revalidate_until > Instant::now()
            && (candidate.entry.headers.contains_key(header::ETAG)
                || candidate.entry.headers.contains_key(header::LAST_MODIFIED))
    }
    pub fn stale(&self, candidate: &Candidate) -> Option<Hit> {
        let mut state = self.state.lock().unwrap();
        if state.epoch != candidate.epoch
            || candidate.entry.stale_until <= Instant::now()
            || state
                .entries
                .get(&candidate.key)
                .is_none_or(|e| e.id != candidate.entry.id)
        {
            return None;
        }
        state.hits += 1;
        state.stale_hits += 1;
        Some(hit(&candidate.entry))
    }
    pub fn invalidate(&self, candidate: &Candidate) {
        let mut state = self.state.lock().unwrap();
        if state.epoch == candidate.epoch
            && state
                .entries
                .get(&candidate.key)
                .is_some_and(|e| e.id == candidate.entry.id)
        {
            state.entries.remove(&candidate.key);
        }
    }
    pub fn revalidated(&self, candidate: &Candidate, headers: &HeaderMap) -> anyhow::Result<Hit> {
        if let (Some(old), Some(new)) = (
            candidate.entry.headers.get(header::ETAG),
            headers.get(header::ETAG),
        ) {
            let weak = |v: &http::HeaderValue| {
                v.as_bytes()
                    .strip_prefix(b"W/")
                    .unwrap_or(v.as_bytes())
                    .to_vec()
            };
            anyhow::ensure!(
                weak(old) == weak(new),
                "304 ETag does not match cached representation"
            );
        }
        if let Some(encoding) = headers.get(header::CONTENT_ENCODING) {
            anyhow::ensure!(
                candidate.entry.headers.get(header::CONTENT_ENCODING) == Some(encoding),
                "304 changed content encoding"
            );
        }
        let mut result = hit(&candidate.entry);
        result.headers.remove(header::AGE);
        result.headers.insert(
            header::DATE,
            http::HeaderValue::from_str(&httpdate::fmt_http_date(SystemTime::now())).unwrap(),
        );
        for name in headers.keys() {
            if matches!(
                name.as_str(),
                "content-length" | "transfer-encoding" | "connection"
            ) {
                continue;
            }
            result.headers.remove(name);
            for value in headers.get_all(name) {
                result.headers.append(name, value.clone());
            }
        }
        // Remove the old representation before capturing updated metadata. A purge
        // racing the validation still prevents publication through its epoch.
        self.invalidate(candidate);
        self.state.lock().unwrap().revalidations += 1;
        Ok(result)
    }
    #[cfg(test)]
    pub fn get(&self, key: &Key) -> Option<Hit> {
        self.lookup(key).0
    }
    #[cfg(test)]
    pub fn capture(self: &Arc<Self>, key: Key, response: &mut Response<Body>) {
        let epoch = self.state.lock().unwrap().epoch;
        self.capture_at(key, epoch, response);
    }
    pub fn capture_at(self: &Arc<Self>, key: Key, epoch: u64, response: &mut Response<Body>) {
        if response.status() != 200
            || response.headers().contains_key(header::SET_COOKIE)
            || response
                .headers()
                .get_all(header::VARY)
                .iter()
                .any(|value| {
                    value.to_str().map_or(true, |value| {
                        value.split(',').any(|name| {
                            !self
                                .config
                                .vary_headers
                                .iter()
                                .any(|allowed| allowed.eq_ignore_ascii_case(name.trim()))
                        })
                    })
                })
            || response.headers().contains_key(header::CONTENT_RANGE)
            || response
                .headers()
                .get(header::CONTENT_TYPE)
                .is_some_and(|h| h.as_bytes().starts_with(b"application/grpc"))
        {
            return;
        }
        let mut shared = false;
        let mut max = None;
        let mut smax = None;
        for value in response.headers().get_all(header::CACHE_CONTROL) {
            let Ok(value) = value.to_str() else {
                return;
            };
            for field in value.split(',') {
                let (name, arg) = field
                    .trim()
                    .split_once('=')
                    .map_or((field.trim(), None), |(a, b)| {
                        (a.trim(), Some(b.trim().trim_matches('"')))
                    });
                if ["no-store", "private", "no-cache"]
                    .iter()
                    .any(|s| name.eq_ignore_ascii_case(s))
                {
                    return;
                }
                if name.eq_ignore_ascii_case("public") {
                    shared = true;
                }
                if name.eq_ignore_ascii_case("max-age") || name.eq_ignore_ascii_case("s-maxage") {
                    let Some(seconds) = arg.and_then(|s| s.parse::<u64>().ok()) else {
                        return;
                    };
                    let dest = if name.eq_ignore_ascii_case("s-maxage") {
                        shared = true;
                        &mut smax
                    } else {
                        &mut max
                    };
                    *dest = Some(dest.unwrap_or(seconds).min(seconds));
                }
            }
        }
        if !shared {
            return;
        }
        let Some(ttl) = smax.or(max) else {
            return;
        };
        if response.headers().get(header::AGE).is_some_and(|h| {
            h.to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .is_none()
        }) || response.headers().get(header::DATE).is_some_and(|h| {
            h.to_str()
                .ok()
                .and_then(|s| httpdate::parse_http_date(s).ok())
                .is_none()
        }) {
            return;
        }
        let age = response
            .headers()
            .get(header::AGE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let apparent = response
            .headers()
            .get(header::DATE)
            .and_then(|h| h.to_str().ok())
            .and_then(|s| httpdate::parse_http_date(s).ok())
            .and_then(|date| SystemTime::now().duration_since(date).ok())
            .map_or(0, |d| d.as_secs());
        let age = age.max(apparent);
        let ttl = ttl.saturating_sub(age).min(self.config.max_ttl_seconds);
        if ttl == 0
            || response
                .body()
                .size_hint()
                .exact()
                .is_some_and(|n| n > self.config.max_object_bytes as u64)
        {
            return;
        }
        let overhead = 512
            + response
                .headers()
                .iter()
                .map(|(k, v)| 64 + k.as_str().len() + v.as_bytes().len())
                .sum::<usize>();
        {
            let mut state = self.state.lock().unwrap();
            if state.epoch != epoch {
                state.discarded_fills += 1;
                return;
            }
            if state.captures >= 128.min(self.config.max_entries)
                || !self.reserve(&mut state, overhead)
            {
                return;
            }
            state.captures += 1;
        }
        let headers = response.headers().clone();
        let version = response.version();
        let base = std::mem::replace(response.body_mut(), Body::text(""));
        *response.body_mut() = Body::custom(base, |body| Tee {
            body: Box::pin(body),
            capture: Some(Capture {
                epoch,
                cache: self.clone(),
                key,
                headers,
                version,
                age,
                ttl,
                created: Instant::now(),
                cost: overhead,
                body: BytesMut::new(),
            }),
            ended: false,
        });
    }
    fn reserve(&self, state: &mut State, amount: usize) -> bool {
        while self.used.load(Ordering::Relaxed).saturating_add(amount) > self.config.max_bytes {
            if !evict(state) {
                return false;
            }
        }
        self.used.fetch_add(amount, Ordering::Relaxed);
        true
    }
    pub fn stats(&self) -> (u64, u64, u64, usize, usize) {
        let s = self.state.lock().unwrap();
        (
            s.hits,
            s.misses,
            s.evictions,
            self.used.load(Ordering::Relaxed),
            s.entries.len(),
        )
    }
    fn status_locked(&self, state: &State) -> CacheStatus {
        CacheStatus {
            epoch: state.epoch,
            hits: state.hits,
            misses: state.misses,
            evictions: state.evictions,
            revalidations: state.revalidations,
            stale_hits: state.stale_hits,
            purges: state.purges,
            purged_entries: state.purged_entries,
            discarded_fills: state.discarded_fills,
            used_bytes: self.used.load(Ordering::Relaxed),
            entries: state.entries.len(),
            active_captures: state.captures,
            background_refreshes: state.refreshing.len(),
            max_bytes: self.config.max_bytes,
            max_entries: self.config.max_entries,
            max_object_bytes: self.config.max_object_bytes,
        }
    }
    pub fn status(&self) -> CacheStatus {
        self.status_locked(&self.state.lock().unwrap())
    }
    pub fn inspect(&self, query: &CacheQuery) -> anyhow::Result<CacheView> {
        query.validate()?;
        let key = query.key.as_deref().map(decode_key).transpose()?;
        let state = self.state.lock().unwrap();
        let now = Instant::now();
        let row = |key: &Key, entry: &Entry| CacheEntry {
            key: encode_key(key),
            body_bytes: entry.data.len(),
            reservation_bytes: entry.reservation.cost,
            age_seconds: entry
                .age
                .saturating_add(now.duration_since(entry.created).as_secs()),
            remaining_ttl_ms: entry
                .expires
                .saturating_duration_since(now)
                .as_millis()
                .min(u64::MAX as u128) as u64,
            expired: entry.expires <= now,
        };
        let entries = if let Some(key) = key {
            state
                .entries
                .get(&key)
                .map(|entry| vec![row(&key, entry)])
                .unwrap_or_default()
        } else {
            state
                .entries
                .iter()
                .take(query.limit)
                .map(|(key, entry)| row(key, entry))
                .collect()
        };
        let truncated = key.is_none() && state.entries.len() > entries.len();
        Ok(CacheView {
            status: self.status_locked(&state),
            entries,
            truncated,
        })
    }
    pub fn purge(&self, key: Option<&str>) -> anyhow::Result<CachePurge> {
        let key = key.map(decode_key).transpose()?;
        let mut state = self.state.lock().unwrap();
        let epoch = state
            .epoch
            .checked_add(1)
            .context("cache purge epoch exhausted")?;
        let removed = if let Some(key) = key {
            usize::from(state.entries.remove(&key).is_some())
        } else {
            let count = state.entries.len();
            state.entries.clear();
            state.order.clear();
            count
        };
        if let Some(key) = key {
            state.order.retain(|(_, entry)| entry != &key);
        }
        state.epoch = epoch;
        state.purges = state.purges.saturating_add(1);
        state.purged_entries = state.purged_entries.saturating_add(removed as u64);
        Ok(CachePurge {
            epoch,
            removed_entries: removed,
            used_bytes_after: self.used.load(Ordering::Relaxed),
            active_captures: state.captures,
            invalidates_pre_purge_fills: true,
        })
    }
}
fn evict(state: &mut State) -> bool {
    while let Some((id, key)) = state.order.pop_front() {
        if state.entries.get(&key).is_some_and(|e| e.id == id) {
            state.entries.remove(&key);
            state.evictions += 1;
            return true;
        }
    }
    false
}
fn stale_budget(config: &prost_config::Cache, headers: &HeaderMap) -> u64 {
    let mut budget = None;
    for value in headers.get_all(header::CACHE_CONTROL) {
        let Ok(value) = value.to_str() else {
            return 0;
        };
        for field in value.split(',') {
            let (name, arg) = field
                .trim()
                .split_once('=')
                .map_or((field.trim(), None), |(n, v)| {
                    (n.trim(), Some(v.trim().trim_matches('"')))
                });
            if [
                "must-revalidate",
                "proxy-revalidate",
                "s-maxage",
                "no-cache",
                "no-store",
                "private",
            ]
            .iter()
            .any(|n| name.eq_ignore_ascii_case(n))
            {
                return 0;
            }
            if name.eq_ignore_ascii_case("stale-if-error") {
                let Some(seconds) = arg.and_then(|v| v.parse::<u64>().ok()) else {
                    return 0;
                };
                budget = Some(budget.unwrap_or(seconds).min(seconds));
            }
        }
    }
    budget.unwrap_or(0).min(config.stale_if_error_seconds)
}
fn retention(config: &prost_config::Cache, headers: &HeaderMap) -> u64 {
    stale_budget(config, headers)
        .max(revalidation_budget(config, headers))
        .max(swr_budget(config, headers))
}
fn swr_budget(config: &prost_config::Cache, headers: &HeaderMap) -> u64 {
    let mut budget = None;
    for value in headers.get_all(header::CACHE_CONTROL) {
        let Ok(value) = value.to_str() else { return 0 };
        for part in value.split(',') {
            let (name, value) = part.trim().split_once('=').unwrap_or((part.trim(), ""));
            if [
                "private",
                "no-store",
                "no-cache",
                "must-revalidate",
                "proxy-revalidate",
                "s-maxage",
            ]
            .iter()
            .any(|v| name.eq_ignore_ascii_case(v))
            {
                return 0;
            }
            if name.eq_ignore_ascii_case("stale-while-revalidate") {
                let Ok(seconds) = value.trim().trim_matches('"').parse::<u64>() else {
                    return 0;
                };
                budget = Some(budget.unwrap_or(seconds).min(seconds));
            }
        }
    }
    budget
        .unwrap_or(0)
        .min(config.stale_while_revalidate_seconds)
}
pub(crate) struct Refresh {
    cache: Arc<Cache>,
    key: Key,
}
impl Drop for Refresh {
    fn drop(&mut self) {
        self.cache
            .state
            .lock()
            .unwrap()
            .refreshing
            .remove(&self.key);
    }
}
fn revalidation_budget(config: &prost_config::Cache, headers: &HeaderMap) -> u64 {
    if config.revalidate
        && (headers.contains_key(header::ETAG) || headers.contains_key(header::LAST_MODIFIED))
    {
        config.revalidate_retention_seconds
    } else {
        0
    }
}
struct Capture {
    epoch: u64,
    created: Instant,
    cache: Arc<Cache>,
    key: Key,
    headers: HeaderMap,
    version: Version,
    age: u64,
    ttl: u64,
    cost: usize,
    body: BytesMut,
}
impl Capture {
    fn append(&mut self, data: &Bytes) -> bool {
        if data.len()
            > self
                .cache
                .config
                .max_object_bytes
                .saturating_sub(self.body.len())
        {
            return false;
        }
        let mut state = self.cache.state.lock().unwrap();
        if state.epoch != self.epoch {
            state.discarded_fills += 1;
            return false;
        }
        if !self.cache.reserve(&mut state, data.len()) {
            return false;
        }
        drop(state);
        self.cost += data.len();
        self.body.extend_from_slice(data);
        true
    }
    fn finish(mut self) {
        if self.created.elapsed() >= Duration::from_secs(self.ttl) {
            return;
        }
        let mut state = self.cache.state.lock().unwrap();
        if state.epoch != self.epoch {
            state.discarded_fills += 1;
            return;
        }
        state.entries.remove(&self.key);
        while state.entries.len() >= self.cache.config.max_entries {
            if !evict(&mut state) {
                break;
            }
        }
        state.sequence = state.sequence.wrapping_add(1);
        let id = state.sequence;
        let created = self.created;
        let retain_until =
            created + Duration::from_secs(self.ttl + retention(&self.cache.config, &self.headers));
        let revalidate_until = created
            + Duration::from_secs(
                self.ttl + revalidation_budget(&self.cache.config, &self.headers),
            );
        let stale_until = created
            + Duration::from_secs(self.ttl + stale_budget(&self.cache.config, &self.headers));
        let data = std::mem::take(&mut self.body).freeze();
        state.entries.insert(
            self.key,
            Entry {
                id,
                headers: std::mem::take(&mut self.headers),
                version: self.version,
                data,
                created,
                expires: created + Duration::from_secs(self.ttl),
                revalidate_until,
                retain_until,
                stale_until,
                age: self.age,
                reservation: Arc::new(Reservation {
                    used: self.cache.used.clone(),
                    cost: self.cost,
                }),
            },
        );
        state.order.push_back((id, self.key));
        self.cost = 0;
        if state.order.len() > 2 * self.cache.config.max_entries {
            let State { entries, order, .. } = &mut *state;
            order.retain(|(id, key)| entries.get(key).is_some_and(|e| e.id == *id));
        }
        drop(state);
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        let mut s = self.cache.state.lock().unwrap();
        self.cache.used.fetch_sub(self.cost, Ordering::Relaxed);
        s.captures -= 1;
    }
}
struct Tee {
    body: Pin<Box<Body>>,
    capture: Option<Capture>,
    ended: bool,
}
impl HttpBody for Tee {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        let frame = self.body.as_mut().poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(f))) => {
                if f.is_trailers() {
                    self.capture.take();
                } else if let Some(data) = f.data_ref() {
                    if self.capture.as_mut().is_some_and(|c| !c.append(data)) {
                        self.capture.take();
                    }
                }
                if self.body.is_end_stream() {
                    self.ended = true;
                    if let Some(c) = self.capture.take() {
                        c.finish();
                    }
                }
            }
            Poll::Ready(None) => {
                self.ended = true;
                if let Some(c) = self.capture.take() {
                    c.finish();
                }
            }
            Poll::Ready(Some(Err(_))) => {
                self.ended = true;
                self.capture.take();
            }
            _ => {}
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.ended
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}
#[cfg(test)]
mod freshness_tests {
    use super::*;
    use http_body_util::BodyExt;
    fn cache() -> Arc<Cache> {
        Cache::new(prost_config::Cache {
            revalidate: true,
            stale_if_error_seconds: 10,
            ..Default::default()
        })
    }
    async fn fill(cache: &Arc<Cache>, control: &str, validator: &str) -> Key {
        let key = Cache::key(&Request::builder().uri("/data").body(()).unwrap()).unwrap();
        let mut response = Response::builder()
            .header(header::CACHE_CONTROL, control)
            .header(
                validator,
                if validator == "etag" {
                    "\"v1\""
                } else {
                    "Wed, 21 Oct 2015 07:28:00 GMT"
                },
            )
            .body(Body::text("saved"))
            .unwrap();
        cache.capture(key, &mut response);
        response.into_body().collect().await.unwrap();
        key
    }
    fn expire(cache: &Cache, key: &Key) {
        let mut state = cache.state.lock().unwrap();
        let entry = state.entries.get_mut(key).unwrap();
        entry.expires = Instant::now() - Duration::from_millis(1);
        entry.stale_until = Instant::now() + Duration::from_secs(10);
    }
    #[tokio::test]
    async fn validators_merge_304_refresh_age_and_keep_representation() {
        for validator in ["etag", "last-modified"] {
            let cache = cache();
            let key = fill(&cache, "public,max-age=60,stale-if-error=20", validator).await;
            expire(&cache, &key);
            let (hit, epoch) = cache.lookup(&key);
            assert!(hit.is_none());
            let candidate = cache.candidate(&key, epoch).unwrap();
            assert!(cache.can_revalidate(&candidate));
            let mut conditional = HeaderMap::new();
            candidate.condition(&mut conditional);
            assert!(conditional.contains_key(if validator == "etag" {
                header::IF_NONE_MATCH
            } else {
                header::IF_MODIFIED_SINCE
            }));
            let mut updated = HeaderMap::new();
            updated.insert(
                header::CACHE_CONTROL,
                http::HeaderValue::from_static("public,max-age=120"),
            );
            updated.insert("x-fresh", http::HeaderValue::from_static("yes"));
            if validator == "etag" {
                updated.insert(header::ETAG, http::HeaderValue::from_static("W/\"v1\""));
            }
            let hit = cache.revalidated(&candidate, &updated).unwrap();
            assert_eq!(hit.data, "saved");
            assert_eq!(hit.headers["x-fresh"], "yes");
            let mut response = Response::new(Body::text("saved"));
            *response.headers_mut() = hit.headers;
            cache.capture_at(key, epoch, &mut response);
            response.into_body().collect().await.unwrap();
            assert_eq!(cache.get(&key).unwrap().headers[header::AGE], "0");
            assert_eq!(cache.status().revalidations, 1);
        }
    }
    #[tokio::test]
    async fn purge_during_revalidation_cannot_resurrect_or_serve_stale() {
        let cache = cache();
        let key = fill(&cache, "public,max-age=60,stale-if-error=20", "etag").await;
        expire(&cache, &key);
        let (_, epoch) = cache.lookup(&key);
        let candidate = cache.candidate(&key, epoch).unwrap();
        cache.purge(None).unwrap();
        assert!(cache.stale(&candidate).is_none());
        let hit = cache.revalidated(&candidate, &HeaderMap::new()).unwrap();
        assert_eq!(hit.data, "saved");
        let mut response = Response::new(Body::text("saved"));
        *response.headers_mut() = hit.headers;
        cache.capture_at(key, epoch, &mut response);
        response.into_body().collect().await.unwrap();
        assert!(cache.get(&key).is_none());
        assert!(cache.status().used_bytes > 0);
        drop(candidate);
        drop(hit.data);
        assert_eq!(cache.status().used_bytes, 0);
    }
    #[tokio::test]
    async fn stale_requires_origin_permission_respects_strict_directives_and_deadline() {
        let config = prost_config::Cache {
            stale_if_error_seconds: 10,
            ..Default::default()
        };
        for (control, expected) in [
            ("public,max-age=60,stale-if-error=20", 10),
            ("public,max-age=60,stale-if-error=3", 3),
            ("public,max-age=60", 0),
            ("public,max-age=60,must-revalidate,stale-if-error=20", 0),
            ("public,s-maxage=60,stale-if-error=20", 0),
            ("public,max-age=60,proxy-revalidate,stale-if-error=20", 0),
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(header::CACHE_CONTROL, control.parse().unwrap());
            assert_eq!(stale_budget(&config, &headers), expected, "{control}");
        }
        let cache = cache();
        let key = fill(&cache, "public,max-age=60,stale-if-error=20", "etag").await;
        expire(&cache, &key);
        let (_, epoch) = cache.lookup(&key);
        let candidate = cache.candidate(&key, epoch).unwrap();
        assert!(cache.stale(&candidate).is_some());
        let mut expired = candidate;
        expired.entry.revalidate_until = Instant::now();
        assert!(!cache.can_revalidate(&expired));
        assert!(cache.stale(&expired).is_some());
        expired.entry.stale_until = Instant::now();
        assert!(cache.stale(&expired).is_none());
        assert_eq!(cache.status().stale_hits, 2);
        let mut bad = HeaderMap::new();
        bad.insert(header::ETAG, "\"different\"".parse().unwrap());
        assert!(cache.revalidated(&expired, &bad).is_err());
        assert_eq!(cache.status().entries, 1);
    }
}
