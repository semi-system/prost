//! Small, synchronous native filters: no event serialization or blocking tasks.
use anyhow::{ensure, Result};
use http::HeaderMap;
use ipnet::IpNet;
use serde::Deserialize;
use std::{
    net::IpAddr,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FilterHook {
    Connection,
    RequestHeaders,
    ResponseHeaders,
}
impl FilterHook {
    pub(crate) fn from_str(hook: &str) -> Option<Self> {
        match hook {
            "connection" => Some(Self::Connection),
            "request_headers" => Some(Self::RequestHeaders),
            "response_headers" => Some(Self::ResponseHeaders),
            _ => None,
        }
    }
}
pub enum FilterDecision {
    Continue,
    Reject {
        status: u16,
        retry_after: Option<u64>,
    },
}
pub trait NativeFilter: Send + Sync {
    fn header_inputs(&self, _: bool) -> &[http::HeaderName] {
        &[]
    }
    fn context_needs(&self) -> u8 {
        0
    }
    fn parent_override(&self) -> Option<(u8, bool)> {
        None
    }
    fn filter_with_context(
        &self,
        call: &FilterCall<'_>,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        self.filter(call.hook, call.peer, headers)
    }
    fn trailers(
        &self,
        _: u16,
        _: Option<&crate::HeaderContext>,
        _: &HeaderMap,
    ) -> Result<Option<HeaderMap>> {
        Ok(None)
    }
    fn applies_to(&self, hook: FilterHook) -> bool;
    /// Must return promptly without blocking. Called on an I/O worker.
    fn filter(
        &self,
        hook: FilterHook,
        peer: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision>;
}
#[derive(Clone, Copy)]
pub struct FilterCall<'a> {
    pub hook: FilterHook,
    pub peer: IpAddr,
    pub status: u16,
    pub context: Option<&'a crate::HeaderContext>,
    pub inherited_skip: u8,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AccessConfig {
    allow: Vec<IpNet>,
    deny: Vec<IpNet>,
}
pub(crate) struct IpAccess {
    config: AccessConfig,
}
impl IpAccess {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: AccessConfig = serde_json::from_value(value)?;
        ensure!(
            config.allow.len() + config.deny.len() <= 256,
            "too many ACL rules"
        );
        Ok(Self { config })
    }
}
impl NativeFilter for IpAccess {
    fn applies_to(&self, hook: FilterHook) -> bool {
        matches!(hook, FilterHook::Connection | FilterHook::RequestHeaders)
    }
    fn filter(
        &self,
        _: FilterHook,
        peer: IpAddr,
        _: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        let allowed = !self.config.deny.iter().any(|n| n.contains(&peer))
            && (self.config.allow.is_empty()
                || self.config.allow.iter().any(|n| n.contains(&peer)));
        Ok(if allowed {
            FilterDecision::Continue
        } else {
            FilterDecision::Reject {
                status: 403,
                retry_after: None,
            }
        })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RateConfig {
    requests_per_second: u64,
    burst: u64,
}
pub(crate) struct RateLimit {
    started: Instant,
    next: AtomicU64,
    spacing: u64,
    allowance: u64,
}
impl RateLimit {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: RateConfig = serde_json::from_value(value)?;
        ensure!(
            (1..=1_000_000).contains(&config.requests_per_second)
                && (1..=1_000_000).contains(&config.burst),
            "invalid rate limit"
        );
        let spacing = 1_000_000_000u64.div_ceil(config.requests_per_second);
        Ok(Self {
            started: Instant::now(),
            next: AtomicU64::new(0),
            spacing,
            allowance: spacing * (config.burst - 1),
        })
    }
    fn admit(&self, now: u64) -> Option<u64> {
        let mut previous = self.next.load(Ordering::Relaxed);
        loop {
            let available = now.saturating_add(self.allowance);
            if previous > available {
                return Some((previous - available).div_ceil(1_000_000_000).max(1));
            }
            let next = previous.max(now).saturating_add(self.spacing);
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
impl NativeFilter for RateLimit {
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == FilterHook::RequestHeaders
    }
    fn filter(
        &self,
        _: FilterHook,
        _: IpAddr,
        _: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        Ok(
            match self.admit(self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64) {
                None => FilterDecision::Continue,
                Some(seconds) => FilterDecision::Reject {
                    status: 429,
                    retry_after: Some(seconds),
                },
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn gcra_burst_refill_and_shared_budget() -> Result<()> {
        let rate = std::sync::Arc::new(RateLimit::new(
            serde_json::json!({"requests_per_second":10,"burst":4}),
        )?);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let rate = rate.clone();
                std::thread::spawn(move || rate.admit(0).is_none())
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
        assert!(rate.admit(99_999_999).is_some());
        assert!(rate.admit(100_000_000).is_none());
        assert!(rate.admit(100_000_000).is_some());
        Ok(())
    }
}
