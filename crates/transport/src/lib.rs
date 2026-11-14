use anyhow::{Context, Result};
use prost_config::{ClientTls, Upstream};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore};
use std::{
    io::BufReader,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub type BoxStream = Box<dyn Stream>;

mod client_hello;
mod dns;
mod h2_headers;
mod policy;
mod proxy_protocol;
mod retry;
pub use client_hello::{ClientHelloGuard, InspectedHello};
pub use dns::{DnsLookup, DnsResolver, LookupFuture};
pub use retry::{connect_tcp, connect_tcp_resolved, DialFailure};
mod tls;
pub use h2_headers::H2HeaderDeadline;
pub use proxy_protocol::{read_proxy, read_proxy_addresses, write_proxy_v2, ProxyAddresses};
mod write_timeout;
pub use tls::server_tls;
pub use write_timeout::TimedWrite;

/// Detect dead peers without waiting for the kernel's long retransmission defaults.
pub fn configure_socket(tcp: &TcpStream) -> Result<()> {
    tcp.set_nodelay(true)?;
    let socket = socket2::SockRef::from(tcp);
    socket.set_tcp_keepalive(
        &socket2::TcpKeepalive::new()
            .with_time(Duration::from_secs(30))
            .with_interval(Duration::from_secs(10))
            .with_retries(3),
    )?;
    #[cfg(target_os = "linux")]
    socket.set_tcp_user_timeout(Some(Duration::from_secs(60)))?;
    Ok(())
}

pub fn client_tls(tls: Option<&ClientTls>) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    if let Some(path) = tls.and_then(|t| t.ca_certificate.as_ref()) {
        let bytes = std::fs::read(path)?;
        for c in rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice())) {
            roots.add(c?)?;
        }
        anyhow::ensure!(
            !roots.is_empty(),
            "CA certificate file contains no certificates"
        );
    } else {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    if let Some(tls) = tls {
        ServerName::try_from(tls.server_name.clone())?;
    }
    let policy = tls.and_then(|t| t.policy.as_ref());
    let builder = ClientConfig::builder_with_provider(policy::provider(policy)?)
        .with_protocol_versions(policy::versions(policy))?
        .with_root_certificates(roots);
    let mut config = if let Some(id) = tls.and_then(|t| t.identity.as_ref()) {
        let certs = rustls_pemfile::certs(&mut BufReader::new(
            std::fs::read(&id.certificate)?.as_slice(),
        ))
        .collect::<std::result::Result<Vec<_>, _>>()?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(
            std::fs::read(&id.private_key)?.as_slice(),
        ))?
        .context("missing client private key")?;
        builder.with_client_auth_cert(certs, key)?
    } else {
        builder.with_no_client_auth()
    };
    if policy.is_some_and(|p| !p.session_resumption) {
        config.resumption = rustls::client::Resumption::disabled();
    }
    Ok(Arc::new(config))
}
pub async fn connect(
    address: &str,
    config: &Upstream,
    tls: Arc<ClientConfig>,
) -> Result<BoxStream> {
    connect_with_proxy(address, config, tls, None).await
}
pub async fn connect_with_proxy(
    address: &str,
    config: &Upstream,
    tls: Arc<ClientConfig>,
    addresses: Option<ProxyAddresses>,
) -> Result<BoxStream> {
    connect_with_resolver(address, config, tls, addresses, None).await
}
pub async fn connect_with_resolver(
    address: &str,
    config: &Upstream,
    tls: Arc<ClientConfig>,
    addresses: Option<ProxyAddresses>,
    resolver: Option<&DnsResolver>,
) -> Result<BoxStream> {
    let budget = Duration::from_millis(config.connect_timeout_ms);
    let started = tokio::time::Instant::now();
    let io = connect_plain_resolved(
        address,
        config.connect_timeout_ms,
        config.send_proxy_v2,
        addresses,
        resolver,
    )
    .await?;
    let remaining = budget.saturating_sub(started.elapsed());
    let io = timeout(remaining, upgrade_tls(io, config.tls.as_ref(), tls))
        .await
        .context("upstream TLS connect timeout")??;
    let io: BoxStream = match config.write_timeout_ms.filter(|ms| *ms > 0) {
        Some(ms) => Box::new(TimedWrite::new(io, Duration::from_millis(ms))),
        None => io,
    };
    Ok(io)
}

pub async fn connect_plain(
    address: &str,
    timeout_ms: u64,
    send_proxy_v2: bool,
    addresses: Option<ProxyAddresses>,
) -> Result<BoxStream> {
    connect_plain_resolved(address, timeout_ms, send_proxy_v2, addresses, None).await
}
pub async fn connect_plain_resolved(
    address: &str,
    timeout_ms: u64,
    send_proxy_v2: bool,
    addresses: Option<ProxyAddresses>,
    resolver: Option<&DnsResolver>,
) -> Result<BoxStream> {
    let budget = Duration::from_millis(timeout_ms);
    let started = tokio::time::Instant::now();
    let mut tcp = timeout(budget, async {
        match resolver {
            Some(dns) => dns.connect(address).await,
            None => TcpStream::connect(address).await,
        }
    })
    .await
    .map_err(|_| {
        DialFailure(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "TCP/DNS connect timeout",
        ))
    })?
    .map_err(DialFailure)?;
    configure_socket(&tcp)?;
    if send_proxy_v2 {
        let addresses = addresses.unwrap_or(ProxyAddresses {
            source: tcp.local_addr()?,
            destination: tcp.peer_addr()?,
        });
        timeout(
            budget.saturating_sub(started.elapsed()),
            write_proxy_v2(&mut tcp, addresses),
        )
        .await
        .context("PROXY header write timeout")??;
    }
    Ok(Box::new(tcp) as BoxStream)
}
pub async fn upgrade_tls(
    io: BoxStream,
    config: Option<&ClientTls>,
    tls: Arc<ClientConfig>,
) -> Result<BoxStream> {
    if let Some(config) = config {
        let name = ServerName::try_from(config.server_name.clone())?;
        Ok(Box::new(TlsConnector::from(tls).connect(name, io).await?))
    } else {
        Ok(io)
    }
}

/// Per-direction idle bounds; half-close drains the opposite direction.
pub async fn relay(a: BoxStream, b: BoxStream, idle: Duration) -> Result<u64> {
    relay_with_timeouts(a, b, idle, None).await
}
pub async fn relay_with_timeouts(
    a: BoxStream,
    b: BoxStream,
    idle: Duration,
    half_close: Option<Duration>,
) -> Result<u64> {
    let (ar, aw) = tokio::io::split(a);
    let (br, bw) = tokio::io::split(b);
    let activity = std::sync::atomic::AtomicU64::new(0);
    let started = Instant::now();
    let first_eof = std::sync::OnceLock::new();
    let fin_changed = tokio::sync::Notify::new();
    async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        mut r: R,
        mut w: W,
        activity: &std::sync::atomic::AtomicU64,
        started: Instant,
        idle: Duration,
        first_eof: &std::sync::OnceLock<Instant>,
        fin_changed: &tokio::sync::Notify,
    ) -> std::io::Result<u64> {
        let mut buf = vec![0u8; 16 * 1024];
        let mut total = 0;
        loop {
            let n = r.read(&mut buf).await?;
            if n == 0 {
                if first_eof.set(Instant::now()).is_ok() {
                    fin_changed.notify_one();
                }
                timeout(idle, w.shutdown()).await.map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::TimedOut, "peer shutdown stalled")
                })??;
                return Ok(total);
            }
            activity.store(
                started.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            timeout(idle, w.write_all(&buf[..n])).await.map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "peer stopped reading")
            })??;
            activity.store(
                started.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            total += n as u64;
        }
    }
    let guard = async {
        loop {
            tokio::time::sleep(idle).await;
            let elapsed = started.elapsed().as_millis() as u64;
            if elapsed.saturating_sub(activity.load(std::sync::atomic::Ordering::Relaxed))
                >= idle.as_millis() as u64
            {
                return Err(anyhow::anyhow!("stream idle timeout"));
            }
        }
    };
    let fin_guard = async {
        let Some(limit) = half_close else {
            return std::future::pending::<Result<u64>>().await;
        };
        loop {
            if let Some(ended) = first_eof.get() {
                tokio::time::sleep_until(tokio::time::Instant::from_std(*ended + limit)).await;
                return Err(anyhow::anyhow!("stream half-close timeout"));
            }
            fin_changed.notified().await;
        }
    };
    tokio::select! {
        result = async { let (a,b) = tokio::try_join!(pump(ar,bw,&activity,started,idle,&first_eof,&fin_changed),pump(br,aw,&activity,started,idle,&first_eof,&fin_changed))?; Ok(a+b) } => result,
        result = guard => result,
        result = fin_guard => result,
    }
}
