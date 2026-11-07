use anyhow::{Context, Result};
use prost_config::{ClientTls, ServerTls, Upstream};
use rustls::{pki_types::ServerName, ClientConfig, RootCertStore, ServerConfig};
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

pub fn server_tls(tls: &ServerTls, http: bool) -> Result<Arc<ServerConfig>> {
    let cert = std::fs::read(&tls.certificate).context("read certificate")?;
    let key = std::fs::read(&tls.private_key).context("read private key")?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(key.as_slice()))?
        .context("missing private key")?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    if http {
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    }
    Ok(Arc::new(config))
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
    let activity = Arc::new(std::sync::Mutex::new(Instant::now()));
    async fn pump<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
        mut r: R,
        mut w: W,
        activity: Arc<std::sync::Mutex<Instant>>,
        idle: Duration,
    ) -> std::io::Result<u64> {
        let mut buf = vec![0u8; 16 * 1024];
        let mut total = 0;
        loop {
            let n = r.read(&mut buf).await?;
            if n == 0 {
                w.shutdown().await?;
                return Ok(total);
            }
            *activity.lock().unwrap() = Instant::now();
            timeout(idle, w.write_all(&buf[..n])).await.map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "peer stopped reading")
            })??;
            *activity.lock().unwrap() = Instant::now();
            total += n as u64;
        }
    }
    let guard = async {
        loop {
            tokio::time::sleep(idle).await;
            if activity.lock().unwrap().elapsed() >= idle {
                return Err(anyhow::anyhow!("stream idle timeout"));
            }
        }
    };
    tokio::select! {
        result = async { let (a,b) = tokio::try_join!(pump(ar,bw,activity.clone(),idle),pump(br,aw,activity.clone(),idle))?; Ok(a+b) } => result,
        result = guard => result,
    }
}
