use anyhow::{ensure, Context, Result};
use http::{header, Request, Uri};
use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper_util::{
    client::legacy::Client,
    rt::{TokioExecutor, TokioIo},
};
use prost_config::{DnsPolicy, TlsSource};
use prost_transport::DnsResolver;
use std::{
    collections::HashMap,
    future::Future,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};
use tokio::net::TcpStream;
use tower_service::Service;

type FetchClient = Client<hyper_rustls::HttpsConnector<Connector>, Empty<Bytes>>;
type ConnectFuture = Pin<
    Box<dyn Future<Output = std::result::Result<TokioIo<TcpStream>, prost_http::Error>> + Send>,
>;
const LIMIT: usize = 16 * 1024 * 1024;
const TOTAL: usize = 32 * 1024 * 1024;
#[derive(Clone)]
struct Connector {
    dns: Arc<DnsResolver>,
}
impl Service<Uri> for Connector {
    type Response = TokioIo<TcpStream>;
    type Error = prost_http::Error;
    type Future = ConnectFuture;
    fn poll_ready(
        &mut self,
        _: &mut TaskContext<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        let dns = self.dns.clone();
        Box::pin(async move {
            let host = uri.host().context("artifact URI has no host")?;
            let port = uri
                .port_u16()
                .unwrap_or(if uri.scheme_str() == Some("https") {
                    443
                } else {
                    80
                });
            let address = if host.contains(':') && !host.starts_with('[') {
                format!("[{host}]:{port}")
            } else {
                format!("{host}:{port}")
            };
            let stream = tokio::time::timeout(Duration::from_secs(3), dns.connect(&address))
                .await
                .context("artifact TCP/DNS timeout")??;
            stream.set_nodelay(true)?;
            Ok(TokioIo::new(stream))
        })
    }
}
fn client(tls: Arc<rustls::ClientConfig>) -> Result<FetchClient> {
    let dns = DnsResolver::new(DnsPolicy {
        max_hosts: 128,
        ..Default::default()
    })?;
    let mut tls = (*tls).clone();
    tls.alpn_protocols.clear();
    let connector = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http()
        .enable_http1()
        .wrap_connector(Connector { dns });
    Ok(Client::builder(TokioExecutor::new())
        .pool_max_idle_per_host(0)
        .http1_max_buf_size(32768)
        .retry_canceled_requests(false)
        .build(connector))
}
/// Owned by a single staging transaction, never by traffic workers.
#[derive(Default)]
pub(super) struct Fetcher {
    s3_config: prost_config::S3Config,
    s3: Option<s3::S3>,
    client: Option<FetchClient>,
    cache: HashMap<String, Arc<Vec<u8>>>,
    bytes: usize,
    started: Option<Instant>,
}
impl Fetcher {
    pub fn new(s3_config: prost_config::S3Config) -> Self {
        Self {
            s3_config,
            ..Default::default()
        }
    }
    pub fn get(&mut self, source: &str) -> Result<Arc<Vec<u8>>> {
        if let Some(bytes) = self.cache.get(source) {
            return Ok(bytes.clone());
        }
        ensure!(
            self.cache.len() < 128,
            "at most 128 remote TLS artifacts per transaction"
        );
        let started = *self.started.get_or_insert_with(Instant::now);
        let remaining = Duration::from_secs(30).saturating_sub(started.elapsed());
        ensure!(
            !remaining.is_zero(),
            "TLS artifact transaction download timeout"
        );
        let handle = tokio::runtime::Handle::try_current()
            .context("remote TLS staging requires a Tokio runtime")?;
        let bytes = handle.block_on(async {
            tokio::time::timeout(remaining.min(Duration::from_secs(10)), async {
                let limit = LIMIT.min(TOTAL - self.bytes);
                match TlsSource::parse(Path::new(source))? {
                    TlsSource::S3(object) => {
                        if self.s3.is_none() {
                            self.s3 = Some(s3::S3::new(&self.s3_config).await?);
                        }
                        self.s3.as_ref().unwrap().get(object, limit).await
                    }
                    _ => {
                        if self.client.is_none() {
                            self.client = Some(client(prost_transport::client_tls(None)?)?);
                        }
                        download(self.client.as_ref().unwrap(), source, limit).await
                    }
                }
            })
            .await
            .context("TLS artifact download timeout")?
        })?;
        self.bytes += bytes.len();
        let bytes = Arc::new(bytes);
        self.cache.insert(source.to_owned(), bytes.clone());
        Ok(bytes)
    }
}
async fn download(client: &FetchClient, source: &str, limit: usize) -> Result<Vec<u8>> {
    let TlsSource::Remote(mut url) = TlsSource::parse(Path::new(source))? else {
        anyhow::bail!("expected remote TLS source");
    };
    for redirects in 0..=3 {
        let request = Request::builder()
            .uri(url.as_str())
            .header(header::ACCEPT_ENCODING, "identity")
            .body(Empty::new())?;
        // Hyper transport errors may retain the request URI. Keep source tokens
        // out of externally returned errors and logs.
        let response = client
            .request(request)
            .await
            .map_err(|_| anyhow::anyhow!("TLS artifact request failed (DNS/TCP/TLS/HTTP)"))?;
        if matches!(response.status().as_u16(), 301 | 302 | 303 | 307 | 308) {
            ensure!(redirects < 3, "TLS artifact redirect limit exceeded");
            let location = response
                .headers()
                .get(header::LOCATION)
                .and_then(|h| h.to_str().ok())
                .context("TLS artifact redirect has no valid Location")?;
            let next = url
                .join(location)
                .context("invalid TLS artifact redirect URI")?;
            ensure!(
                !(url.scheme() == "https" && next.scheme() != "https"),
                "TLS artifact HTTPS redirect downgrade rejected"
            );
            let TlsSource::Remote(next) = TlsSource::parse(Path::new(next.as_str()))? else {
                anyhow::bail!("TLS artifact redirect requires HTTP(S)");
            };
            url = next;
            continue;
        }
        return read(response, limit).await;
    }
    unreachable!()
}
async fn read(
    mut response: http::Response<hyper::body::Incoming>,
    limit: usize,
) -> Result<Vec<u8>> {
    ensure!(
        response.status() == 200,
        "TLS artifact HTTP status {}",
        response.status().as_u16()
    );
    ensure!(
        response
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_none_or(|v| v == "identity"),
        "TLS artifact content encoding must be identity"
    );
    if let Some(size) = response.headers().get(header::CONTENT_LENGTH) {
        let size = size
            .to_str()?
            .parse::<u64>()
            .context("invalid TLS artifact Content-Length")?;
        ensure!(size <= limit as u64, "TLS artifact exceeds download budget");
    }
    let mut bytes = Vec::new();
    while let Some(frame) = response.body_mut().frame().await {
        let frame = frame.map_err(|_| anyhow::anyhow!("TLS artifact body read failed"))?;
        if let Ok(data) = frame.into_data() {
            ensure!(
                data.len() <= limit.saturating_sub(bytes.len()),
                "TLS artifact exceeds download budget"
            );
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}
#[path = "s3.rs"]
mod s3;
#[cfg(test)]
#[path = "fetch_tests.rs"]
mod tests;
