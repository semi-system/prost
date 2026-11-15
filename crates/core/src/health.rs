use crate::PreparedUpstream;
use anyhow::{Context, Result};
use http::Request;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
mod mail;

#[derive(Clone, Default)]
struct Window {
    failures: VecDeque<Instant>,
    until: Option<Instant>,
}
enum Passive {
    Legacy,
    Disabled,
    Windowed {
        max_fails: usize,
        timeout: Duration,
        state: Mutex<Window>,
    },
}

pub(crate) struct State {
    pub healthy: AtomicBool,
    failures: AtomicU32,
    successes: AtomicU32,
    active_healthy: AtomicBool,
    passive: Passive,
    pub observed: Arc<crate::observability::Health>,
}
impl State {
    pub fn snapshot(&self) -> Self {
        let passive = match &self.passive {
            Passive::Legacy => Passive::Legacy,
            Passive::Disabled => Passive::Disabled,
            Passive::Windowed {
                max_fails,
                timeout,
                state,
            } => Passive::Windowed {
                max_fails: *max_fails,
                timeout: *timeout,
                state: Mutex::new(state.lock().unwrap_or_else(|e| e.into_inner()).clone()),
            },
        };
        let ejected = match &passive {
            Passive::Windowed { state, .. } => state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .until
                .is_some(),
            _ => false,
        };
        Self {
            healthy: AtomicBool::new(!ejected && self.healthy.load(Ordering::Relaxed)),
            failures: AtomicU32::new(self.failures.load(Ordering::Relaxed)),
            successes: AtomicU32::new(self.successes.load(Ordering::Relaxed)),
            active_healthy: AtomicBool::new(self.active_healthy.load(Ordering::Relaxed)),
            passive,
            observed: self.observed.clone(),
        }
    }
    pub fn new(policy: Option<&prost_config::PassiveHealth>) -> Self {
        Self {
            healthy: AtomicBool::new(true),
            failures: AtomicU32::new(0),
            successes: AtomicU32::new(0),
            active_healthy: AtomicBool::new(true),
            passive: match policy {
                None => Passive::Legacy,
                Some(p) if p.max_fails == 0 => Passive::Disabled,
                Some(p) => Passive::Windowed {
                    max_fails: p.max_fails as usize,
                    timeout: Duration::from_millis(p.fail_timeout_ms),
                    state: Mutex::new(Window::default()),
                },
            },
            observed: Arc::new(crate::observability::Health::default()),
        }
    }
    pub fn reconfigure(&self, policy: Option<&prost_config::PassiveHealth>) -> Self {
        let mut state = Self::new(policy);
        state.observed = self.observed.clone();
        let active = self.active_healthy.load(Ordering::Relaxed);
        state.active_healthy.store(active, Ordering::Relaxed);
        state.healthy.store(active, Ordering::Relaxed);
        state
            .failures
            .store(self.failures.load(Ordering::Relaxed), Ordering::Relaxed);
        state
            .successes
            .store(self.successes.load(Ordering::Relaxed), Ordering::Relaxed);
        state
    }
    pub fn observe(&self, ok: bool, policy: &prost_config::HealthCheck) {
        self.observed.probes[usize::from(!ok)].fetch_add(1, Ordering::Relaxed);
        if ok {
            self.failures.store(0, Ordering::Relaxed);
            if self.successes.fetch_add(1, Ordering::Relaxed) >= policy.rise - 1 {
                self.successes.store(policy.rise, Ordering::Relaxed);
                self.active_healthy.store(true, Ordering::Relaxed);
                self.recover();
            }
        } else {
            self.successes.store(0, Ordering::Relaxed);
            if self.failures.fetch_add(1, Ordering::Relaxed) >= policy.fall - 1 {
                self.failures.store(policy.fall, Ordering::Relaxed);
                self.active_healthy.store(false, Ordering::Relaxed);
                self.healthy.store(false, Ordering::Relaxed);
            }
        }
    }
    pub fn passive_failure(&self) {
        match &self.passive {
            Passive::Legacy => {
                self.observed.failures.fetch_add(1, Ordering::Relaxed);
                self.successes.store(0, Ordering::Relaxed);
                if self.healthy.swap(false, Ordering::Relaxed) {
                    self.observed.ejections.fetch_add(1, Ordering::Relaxed);
                }
            }
            Passive::Disabled => {}
            Passive::Windowed {
                max_fails,
                timeout,
                state,
            } => {
                self.observed.failures.fetch_add(1, Ordering::Relaxed);
                let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                // In-flight failures never extend an existing ejection. A fresh
                // successful active probe is required after the deadline.
                if state.until.is_some() {
                    return;
                }
                while state
                    .failures
                    .front()
                    .is_some_and(|t| now.duration_since(*t) >= *timeout)
                {
                    state.failures.pop_front();
                }
                state.failures.push_back(now);
                if state.failures.len() >= *max_fails {
                    state.failures.clear();
                    state.until = Some(now + *timeout);
                    self.observed.ejections.fetch_add(1, Ordering::Relaxed);
                    self.successes.store(0, Ordering::Relaxed);
                    self.healthy.store(false, Ordering::Relaxed);
                }
            }
        }
    }
    fn recover(&self) {
        if let Passive::Windowed { state, .. } = &self.passive {
            let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
            if state.until.is_some_and(|until| Instant::now() < until) {
                return;
            }
            state.until = None;
            self.healthy.store(
                self.active_healthy.load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
        } else {
            self.healthy.store(true, Ordering::Relaxed);
        }
    }
    pub fn passive_status(&self) -> Option<serde_json::Value> {
        match &self.passive {
            Passive::Legacy => None,
            Passive::Disabled => Some(
                serde_json::json!({"enabled":false,"ejected":false,"recent_failures":0,"cooldown_remaining_ms":0}),
            ),
            Passive::Windowed {
                max_fails,
                timeout,
                state,
            } => {
                let state = state.lock().unwrap_or_else(|e| e.into_inner());
                let now = Instant::now();
                let recent = state
                    .failures
                    .iter()
                    .filter(|t| now.duration_since(**t) < *timeout)
                    .count();
                let remaining = state.until.map_or(0, |t| {
                    t.saturating_duration_since(now)
                        .as_nanos()
                        .div_ceil(1_000_000) as u64
                });
                Some(
                    serde_json::json!({"enabled":true,"max_fails":max_fails,"fail_timeout_ms":timeout.as_millis() as u64,"ejected":state.until.is_some(),"recent_failures":recent,"cooldown_remaining_ms":remaining}),
                )
            }
        }
    }
}

// Dropping a timed-out/cancelled probe must also stop its HTTP driver.
struct Driver(tokio::task::JoinHandle<()>);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) async fn probe(upstream: &PreparedUpstream, index: usize) -> bool {
    let policy = &upstream.config.health_check;
    tokio::time::timeout(Duration::from_millis(policy.timeout_ms), async {
        let endpoint = &upstream.config.endpoints[index];
        let is_http =
            policy.path.is_some() || policy.protocol == prost_config::HealthProtocol::Http;
        let starttls = matches!(
            policy.protocol,
            prost_config::HealthProtocol::SmtpStarttls
                | prost_config::HealthProtocol::ImapStarttls
                | prost_config::HealthProtocol::Pop3Starttls
        );
        let tls_config = policy.tls.as_ref().or(upstream.config.tls.as_ref());
        let tls = if is_http && upstream.config.http2 {
            let mut tls = (*upstream.health_tls).clone();
            tls.alpn_protocols = vec![b"h2".to_vec()];
            Arc::new(tls)
        } else {
            upstream.health_tls.clone()
        };
        let address = if let Some(port) = policy.port {
            format!(
                "{}:{port}",
                endpoint
                    .address
                    .rsplit_once(':')
                    .context("health endpoint address")?
                    .0
            )
        } else {
            endpoint.address.clone()
        };
        let stream = prost_transport::connect_plain_with_proxy_command(
            &address,
            upstream.config.connect_timeout_ms,
            policy
                .send_proxy_v2
                .unwrap_or(upstream.config.send_proxy_v2)
                .then_some(policy.proxy_v2_command),
            None,
            upstream.dns.as_deref(),
        )
        .await?;
        let stream = if starttls {
            stream
        } else {
            prost_transport::upgrade_tls(stream, tls_config, tls.clone()).await?
        };
        if !matches!(
            policy.protocol,
            prost_config::HealthProtocol::Connect | prost_config::HealthProtocol::Http
        ) {
            return mail::probe(stream, policy, tls_config, tls).await;
        }
        if !is_http {
            return Ok::<_, anyhow::Error>(true);
        }
        let path = policy.path.as_deref().unwrap_or("/");
        let host = policy.host.as_deref().unwrap_or(&endpoint.address);
        let request = Request::builder()
            .uri(path)
            .header("host", host)
            .body(Full::new(Bytes::new()))?;
        let status = if upstream.config.http2 {
            let (mut sender, conn) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                    .await?;
            let _driver = Driver(tokio::spawn(async move {
                let _ = conn.await;
            }));
            sender.send_request(request).await?.status()
        } else {
            let (mut sender, conn) =
                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
            let _driver = Driver(tokio::spawn(async move {
                let _ = conn.await;
            }));
            sender.send_request(request).await?.status()
        };
        Ok(status.as_u16() == policy.expected_status)
    })
    .await
    .is_ok_and(|r: Result<bool>| r.unwrap_or(false))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn thresholds_require_consecutive_observations() {
        let state = State::new(None);
        let policy = prost_config::HealthCheck {
            fall: 2,
            rise: 2,
            ..Default::default()
        };
        state.observe(false, &policy);
        assert!(state.healthy.load(Ordering::Relaxed));
        state.observe(true, &policy);
        state.observe(false, &policy);
        assert!(state.healthy.load(Ordering::Relaxed));
        state.observe(false, &policy);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(true, &policy);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(false, &policy);
        state.observe(true, &policy);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(true, &policy);
        assert!(state.healthy.load(Ordering::Relaxed));
    }
    #[tokio::test(start_paused = true)]
    async fn passive_rolling_window_cooldown_and_probe_recovery() {
        let p = prost_config::PassiveHealth {
            max_fails: 2,
            fail_timeout_ms: 1000,
            failure_statuses: vec![],
        };
        let active = prost_config::HealthCheck {
            rise: 2,
            ..Default::default()
        };
        let state = State::new(Some(&p));
        state.passive_failure();
        assert_eq!(state.passive_status().unwrap()["recent_failures"], 1);
        tokio::time::advance(Duration::from_millis(1000)).await;
        state.passive_failure(); // Exactly-expired failure does not count.
        assert!(state.healthy.load(Ordering::Relaxed));
        assert_eq!(state.passive_status().unwrap()["recent_failures"], 1);
        state.passive_failure();
        assert!(!state.healthy.load(Ordering::Relaxed));
        assert_eq!(
            state.passive_status().unwrap()["cooldown_remaining_ms"],
            1000
        );
        state.observe(true, &active);
        state.observe(true, &active);
        assert!(!state.healthy.load(Ordering::Relaxed)); // Probes cannot bypass cooldown.
        tokio::time::advance(Duration::from_millis(999)).await;
        state.passive_failure(); // Outstanding requests do not extend isolation.
        let copy = state.snapshot();
        assert_eq!(copy.passive_status().unwrap()["cooldown_remaining_ms"], 1);
        state.observe(true, &active);
        assert!(!state.healthy.load(Ordering::Relaxed));
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!state.healthy.load(Ordering::Relaxed)); // Deadline alone does not recover.
        state.observe(false, &active);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(true, &active);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(true, &active);
        assert!(state.healthy.load(Ordering::Relaxed));
        assert_eq!(state.passive_status().unwrap()["ejected"], false);
        assert!(!copy.healthy.load(Ordering::Relaxed)); // Generation observations are independent.
        copy.observe(true, &active);
        assert!(copy.healthy.load(Ordering::Relaxed));
    }
    #[tokio::test(start_paused = true)]
    async fn disabled_passive_health_keeps_active_probes_and_legacy_mode() {
        let policy = prost_config::PassiveHealth {
            max_fails: 0,
            ..Default::default()
        };
        let state = State::new(Some(&policy));
        for _ in 0..100 {
            state.passive_failure();
        }
        assert!(state.healthy.load(Ordering::Relaxed));
        assert_eq!(state.passive_status().unwrap()["enabled"], false);
        let active = prost_config::HealthCheck::default();
        state.observe(false, &active);
        assert!(!state.healthy.load(Ordering::Relaxed));
        state.observe(true, &active);
        assert!(state.healthy.load(Ordering::Relaxed));
        let legacy = State::new(None);
        legacy.passive_failure();
        assert!(!legacy.healthy.load(Ordering::Relaxed));
        assert!(legacy.passive_status().is_none());
        legacy.observe(true, &active);
        assert!(legacy.healthy.load(Ordering::Relaxed));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_failures_eject_once_without_extending_deadline() {
        let policy = prost_config::PassiveHealth {
            max_fails: 10,
            fail_timeout_ms: 60000,
            failure_statuses: vec![],
        };
        let state = Arc::new(State::new(Some(&policy)));
        let barrier = Arc::new(tokio::sync::Barrier::new(128));
        let mut tasks = Vec::new();
        for _ in 0..128 {
            let state = state.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                state.passive_failure();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert!(!state.healthy.load(Ordering::Relaxed));
        let Passive::Windowed { state: window, .. } = &state.passive else {
            panic!("windowed mode")
        };
        let until = window.lock().unwrap().until.unwrap();
        for _ in 0..10000 {
            state.passive_failure();
        }
        let window = window.lock().unwrap();
        assert_eq!(window.until, Some(until));
        assert!(window.failures.is_empty());
        assert!(window.failures.capacity() <= 16); // No storage grows with error volume.
    }
}
