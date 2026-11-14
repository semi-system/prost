use crate::{BoxStream, ProxyAddresses};
use anyhow::{Context, Result};
use prost_config::Upstream;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

/// Only errors produced before a TCP socket was established are retryable.
#[derive(Debug)]
pub struct DialFailure(pub(crate) std::io::Error);
impl std::fmt::Display for DialFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "upstream TCP dial: {}", self.0)
    }
}
impl std::error::Error for DialFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl DialFailure {
    fn retryable(&self) -> bool {
        !matches!(
            self.0.kind(),
            std::io::ErrorKind::PermissionDenied
                | std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::Unsupported
        )
    }
}
pub async fn connect_tcp(
    address: &str,
    config: &Upstream,
    tls: Arc<rustls::ClientConfig>,
    addresses: Option<ProxyAddresses>,
    retries: &AtomicU64,
) -> Result<BoxStream> {
    connect_tcp_resolved(address, config, tls, addresses, retries, None).await
}
pub async fn connect_tcp_resolved(
    address: &str,
    config: &Upstream,
    tls: Arc<rustls::ClientConfig>,
    addresses: Option<ProxyAddresses>,
    retries: &AtomicU64,
    resolver: Option<&crate::DnsResolver>,
) -> Result<BoxStream> {
    let Some(policy) = &config.tcp_retry else {
        return crate::connect_with_resolver(address, config, tls, addresses, resolver).await;
    };
    tokio::time::timeout(Duration::from_millis(policy.timeout_ms), async {
        for attempt in 0..policy.max_attempts {
            if attempt > 0 {
                if policy.backoff_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(policy.backoff_ms)).await;
                }
                retries.fetch_add(1, Ordering::Relaxed);
            }
            match crate::connect_with_resolver(address, config, tls.clone(), addresses, resolver)
                .await
            {
                Ok(io) => return Ok(io),
                Err(error) => {
                    if attempt + 1 == policy.max_attempts
                        || !error.chain().any(|cause| {
                            cause
                                .downcast_ref::<DialFailure>()
                                .is_some_and(DialFailure::retryable)
                        })
                    {
                        return Err(error);
                    }
                }
            }
        }
        unreachable!("validated max_attempts")
    })
    .await
    .context("TCP retry total timeout")?
}
