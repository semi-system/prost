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

mod tls;
pub use tls::server_tls;

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
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = tls.and_then(|t| t.ca_certificate.as_ref()) {
        let bytes = std::fs::read(path)?;
        for c in rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice())) {
            roots.add(c?)?;
        }
    }
    if let Some(tls) = tls {
        ServerName::try_from(tls.server_name.clone())?;
    }
    Ok(Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ))
}
pub async fn connect(
    address: &str,
    config: &Upstream,
    tls: Arc<ClientConfig>,
) -> Result<BoxStream> {
    timeout(Duration::from_millis(config.connect_timeout_ms), async {
        let tcp = TcpStream::connect(address).await?;
        configure_socket(&tcp)?;
        let io: BoxStream = if let Some(t) = &config.tls {
            let name = ServerName::try_from(t.server_name.clone())?;
            Box::new(TlsConnector::from(tls).connect(name, tcp).await?)
        } else {
            Box::new(tcp)
        };
        Ok::<_, anyhow::Error>(io)
    })
    .await
    .context("connect timeout")?
}

/// Per-direction idle bounds; half-close drains the opposite direction.
pub async fn relay(a: BoxStream, b: BoxStream, idle: Duration) -> Result<u64> {
    let (ar, aw) = tokio::io::split(a);
    let (br, bw) = tokio::io::split(b);
    let activity = std::sync::atomic::AtomicU64::new(0);
    let started = Instant::now();
    async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        mut r: R,
        mut w: W,
        activity: &std::sync::atomic::AtomicU64,
        started: Instant,
        idle: Duration,
    ) -> std::io::Result<u64> {
        let mut buf = vec![0u8; 16 * 1024];
        let mut total = 0;
        loop {
            let n = r.read(&mut buf).await?;
            if n == 0 {
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
    tokio::select! {
        result = async { let (a,b) = tokio::try_join!(pump(ar,bw,&activity,started,idle),pump(br,aw,&activity,started,idle))?; Ok(a+b) } => result,
        result = guard => result,
    }
}
