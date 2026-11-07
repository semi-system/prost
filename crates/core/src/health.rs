use crate::PreparedUpstream;
use anyhow::Result;
use http::Request;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct State {
    pub healthy: AtomicBool,
    failures: AtomicU32,
    successes: AtomicU32,
}
impl State {
    pub fn snapshot(&self) -> Self {
        Self {
            healthy: AtomicBool::new(self.healthy.load(Ordering::Relaxed)),
            failures: AtomicU32::new(self.failures.load(Ordering::Relaxed)),
            successes: AtomicU32::new(self.successes.load(Ordering::Relaxed)),
        }
    }
    pub fn new() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            failures: AtomicU32::new(0),
            successes: AtomicU32::new(0),
        }
    }
    pub fn observe(&self, ok: bool, policy: &prost_config::HealthCheck) {
        if ok {
            self.failures.store(0, Ordering::Relaxed);
            if self.successes.fetch_add(1, Ordering::Relaxed) >= policy.rise - 1 {
                self.successes.store(policy.rise, Ordering::Relaxed);
                self.healthy.store(true, Ordering::Relaxed);
            }
        } else {
            self.successes.store(0, Ordering::Relaxed);
            if self.failures.fetch_add(1, Ordering::Relaxed) >= policy.fall - 1 {
                self.failures.store(policy.fall, Ordering::Relaxed);
                self.healthy.store(false, Ordering::Relaxed);
            }
        }
    }
    pub fn passive_failure(&self) {
        self.successes.store(0, Ordering::Relaxed);
        self.healthy.store(false, Ordering::Relaxed);
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
        let tls = if policy.path.is_some() && upstream.config.http2 {
            let mut tls = (*upstream.tls).clone();
            tls.alpn_protocols = vec![b"h2".to_vec()];
            Arc::new(tls)
        } else {
            upstream.tls.clone()
        };
        let stream = prost_transport::connect(&endpoint.address, &upstream.config, tls).await?;
        let Some(path) = &policy.path else {
            return Ok::<_, anyhow::Error>(true);
        };
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
        let state = State::new();
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
}
