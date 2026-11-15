use anyhow::{ensure, Result};
mod body;
mod cache;
pub mod cache_control;
#[cfg(test)]
mod cache_tests;
mod capture;
mod compression;
#[cfg(test)]
mod dns_tests;
pub mod h2_deadline;
pub mod keepalive;
mod observe;
mod policy;
pub use policy::{CachedResponse, HttpResources, PolicyStats};
mod pool_stats;
pub use observe::{BodyEnd, BodyObserver};
mod upload;
pub use body::{Body, KeepAlive};
use body::{Completion, Guard};
use http::{header, HeaderMap, HeaderName, HeaderValue, Request, Response, Uri};
use hyper::body::Body as _;
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioIo,
};
pub use pool_stats::{PoolSnapshot, PoolStats};
use prost_config::{Route, Upstream};
use std::{sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;
pub use upload::{HttpFailure, RequestBody, RequestPolicy};

pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Debug)]
pub struct ProxyCapacity;
impl std::fmt::Display for ProxyCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("scoped PROXY transport capacity exhausted")
    }
}
impl std::error::Error for ProxyCapacity {}
/// Local socket/resource exhaustion must not eject a healthy backend.
pub fn local_capacity_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        if cause.is::<ProxyCapacity>() {
            return true;
        }
        let Some(error) = cause.downcast_ref::<std::io::Error>() else {
            return false;
        };
        if matches!(
            error.kind(),
            std::io::ErrorKind::AddrNotAvailable | std::io::ErrorKind::OutOfMemory
        ) {
            return true;
        }
        #[cfg(unix)]
        if matches!(
            error.raw_os_error(),
            Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS)
        ) {
            return true;
        }
        false
    })
}
pub type HttpClient = Client<connector::Connector, RequestBody>;
mod connector;
thread_local! { static WORKER_SLOT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
pub fn set_worker_slot(slot: usize) {
    WORKER_SLOT.with(|current| current.set(slot));
}
#[inline]
pub fn worker_slot() -> usize {
    WORKER_SLOT.with(std::cell::Cell::get)
}
pub struct ClientPool {
    clients: Vec<HttpClient>,
    dynamic: Option<DynamicPool>,
}
struct DynamicPool {
    configs: Vec<Upstream>,
    tls: Arc<rustls::ClientConfig>,
    stats: Arc<PoolStats>,
    dns: Arc<prost_transport::DnsResolver>,
    slots: std::sync::Mutex<Vec<(u64, HttpClient)>>,
}
impl ClientPool {
    pub fn worker_count(&self) -> usize {
        self.clients.len()
    }
    pub fn new(
        upstream: &Upstream,
        tls: Arc<rustls::ClientConfig>,
        workers: usize,
    ) -> Result<Self> {
        Self::new_with_stats(upstream, tls, workers, Arc::new(PoolStats::new(workers)))
    }
    pub fn new_with_stats(
        upstream: &Upstream,
        tls: Arc<rustls::ClientConfig>,
        workers: usize,
        stats: Arc<PoolStats>,
    ) -> Result<Self> {
        Self::new_with_dns(
            upstream,
            tls,
            workers,
            stats,
            upstream
                .dns
                .clone()
                .map(prost_transport::DnsResolver::new)
                .transpose()?,
        )
    }
    pub fn new_with_dns(
        upstream: &Upstream,
        tls: Arc<rustls::ClientConfig>,
        workers: usize,
        stats: Arc<PoolStats>,
        dns: Option<Arc<prost_transport::DnsResolver>>,
    ) -> Result<Self> {
        ensure!(
            (1..=32).contains(&workers),
            "invalid client pool worker count"
        );
        let mut clients = Vec::with_capacity(workers);
        let mut configs = Vec::new();
        for index in 0..workers {
            let mut config = upstream.clone();
            config.pool_max_idle_per_host = upstream.pool_max_idle_per_host / workers
                + usize::from(index < upstream.pool_max_idle_per_host % workers);
            clients.push(client_observed(
                &config,
                tls.clone(),
                stats.clone(),
                index,
                None,
                None,
                dns.clone(),
            )?);
            configs.push(config);
        }
        let dynamic = dns.map(|dns| DynamicPool {
            configs,
            tls,
            stats,
            dns,
            slots: std::sync::Mutex::new(clients.iter().map(|c| (0, c.clone())).collect()),
        });
        Ok(Self { clients, dynamic })
    }
    pub fn current(&self) -> &HttpClient {
        WORKER_SLOT.with(|slot| &self.clients[slot.get() % self.clients.len()])
    }
    pub fn current_dns(&self) -> Result<HttpClient> {
        let Some(dynamic) = &self.dynamic else {
            return Ok(self.current().clone());
        };
        let worker = worker_slot() % self.clients.len();
        let revision = dynamic.dns.revision();
        let mut slots = dynamic.slots.lock().unwrap();
        if slots[worker].0 != revision {
            slots[worker] = (
                revision,
                client_observed(
                    &dynamic.configs[worker],
                    dynamic.tls.clone(),
                    dynamic.stats.clone(),
                    worker,
                    None,
                    None,
                    Some(dynamic.dns.clone()),
                )?,
            );
        }
        Ok(slots[worker].1.clone())
    }
}
static FORWARDED_HOP_HEADERS: [HeaderName; 9] = [
    header::CONNECTION,
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    header::PROXY_AUTHENTICATE,
    header::PROXY_AUTHORIZATION,
    header::TE,
    header::TRAILER,
    header::TRANSFER_ENCODING,
    header::UPGRADE,
];

pub fn client(upstream: &Upstream, tls: Arc<rustls::ClientConfig>) -> Result<HttpClient> {
    ensure!(
        !upstream.send_proxy_v2,
        "use a per-frontend client_with_proxy for PROXY v2"
    );
    client_observed(
        upstream,
        tls,
        Arc::new(PoolStats::default()),
        0,
        None,
        None,
        upstream
            .dns
            .clone()
            .map(prost_transport::DnsResolver::new)
            .transpose()?,
    )
}
pub fn client_with_proxy(
    upstream: &Upstream,
    tls: Arc<rustls::ClientConfig>,
    stats: Arc<PoolStats>,
    worker: usize,
    addresses: prost_transport::ProxyAddresses,
    capacity: Arc<tokio::sync::Semaphore>,
) -> Result<HttpClient> {
    client_with_proxy_dns(
        upstream,
        tls,
        stats,
        worker,
        addresses,
        capacity,
        upstream
            .dns
            .clone()
            .map(prost_transport::DnsResolver::new)
            .transpose()?,
    )
}
pub fn client_with_proxy_dns(
    upstream: &Upstream,
    tls: Arc<rustls::ClientConfig>,
    stats: Arc<PoolStats>,
    worker: usize,
    addresses: prost_transport::ProxyAddresses,
    capacity: Arc<tokio::sync::Semaphore>,
    dns: Option<Arc<prost_transport::DnsResolver>>,
) -> Result<HttpClient> {
    ensure!(
        upstream.send_proxy_v2,
        "PROXY context requires send_proxy_v2"
    );
    let mut scoped = upstream.clone();
    // One HTTP/2 frontend can fan out to several HTTP/1 backend transports.
    // Evicting all but one idle socket after each response causes TIME_WAIT
    // churn and exhausts ephemeral ports under sustained multiplexed traffic.
    // Identity remains frontend-scoped and the process semaphore caps all
    // active/idle PROXY sockets together.
    scoped.pool_max_idle_per_host = scoped.pool_max_idle_per_host.min(32);
    scoped.pool_idle_timeout_ms = scoped.pool_idle_timeout_ms.min(5000);
    client_observed(
        &scoped,
        tls,
        stats,
        worker,
        Some(addresses),
        Some(capacity),
        dns,
    )
}
fn client_observed(
    upstream: &Upstream,
    tls: Arc<rustls::ClientConfig>,
    stats: Arc<PoolStats>,
    worker: usize,
    addresses: Option<prost_transport::ProxyAddresses>,
    capacity: Option<Arc<tokio::sync::Semaphore>>,
    dns: Option<Arc<prost_transport::DnsResolver>>,
) -> Result<HttpClient> {
    let mut connector = HttpConnector::new();
    connector.enforce_http(false);
    connector.set_connect_timeout(Some(Duration::from_millis(upstream.connect_timeout_ms)));
    connector.set_nodelay(true);
    let connector = connector::ProxyTcp {
        inner: connector,
        addresses,
        required: upstream.send_proxy_v2,
        dns,
    };
    let mut tls = (*tls).clone();
    tls.alpn_protocols.clear();
    let builder = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_or_http();
    let connector = if let Some(t) = &upstream.tls {
        let name = rustls::pki_types::ServerName::try_from(t.server_name.clone())?;
        let builder =
            builder.with_server_name_resolver(move |_: &Uri| Ok::<_, Error>(name.clone()));
        if upstream.http2 {
            builder.enable_http2().wrap_connector(connector)
        } else {
            builder.enable_http1().wrap_connector(connector)
        }
    } else if upstream.http2 {
        builder.enable_http2().wrap_connector(connector)
    } else {
        builder.enable_http1().wrap_connector(connector)
    };
    let mut builder = Client::builder(hyper_util::rt::TokioExecutor::new());
    builder
        .pool_timer(hyper_util::rt::TokioTimer::new())
        .http1_max_buf_size(upstream.http1_max_buf_size)
        .pool_max_idle_per_host(upstream.pool_max_idle_per_host)
        .pool_idle_timeout(Duration::from_millis(upstream.pool_idle_timeout_ms))
        .retry_canceled_requests(false);
    let h2 = &upstream.http2_settings;
    builder
        .http2_initial_stream_window_size(h2.initial_stream_window_size)
        .http2_initial_connection_window_size(h2.initial_connection_window_size)
        .http2_max_concurrent_streams(h2.max_concurrent_streams)
        .http2_adaptive_window(h2.adaptive_window);
    if upstream.http2 {
        builder.http2_only(true);
    }
    Ok(builder.build(connector::Connector::new(
        connector, upstream, stats, worker, capacity,
    )))
}
pub fn response(status: u16, text: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::text(text))
        .unwrap()
}

/// Removes nominated connection headers and all hop-by-hop headers. TE trailers is allowed for gRPC requests.
pub fn sanitize(headers: &mut HeaderMap, upgrade: bool, request: bool) {
    let nominated: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
        .filter(|s| !s.trim().eq_ignore_ascii_case("keep-alive"))
        .filter_map(|s| HeaderName::from_bytes(s.trim().as_bytes()).ok())
        .collect();
    for name in nominated {
        if !(upgrade && name == header::UPGRADE) {
            headers.remove(name);
        }
    }
    let trailers = request
        && headers
            .get(header::TE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("trailers"));
    for name in &FORWARDED_HOP_HEADERS {
        if upgrade && name == header::UPGRADE {
            continue;
        }
        headers.remove(name);
    }
    if trailers {
        headers.insert(header::TE, HeaderValue::from_static("trailers"));
    }
    if upgrade {
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    }
}
pub fn select_route<'a>(routes: &'a [Route], host: &str, path: &str) -> Option<&'a Route> {
    routes
        .iter()
        .enumerate()
        .filter(|(_, r)| {
            r.host.as_ref().is_none_or(|h| h.eq_ignore_ascii_case(host))
                && path.starts_with(&r.path_prefix)
        })
        .max_by_key(|(i, r)| (r.priority, r.path_prefix.len(), std::cmp::Reverse(*i)))
        .map(|(_, r)| r)
}

pub struct Outcome {
    pub response: Response<Body>,
    pub tunnel: Option<tokio::task::JoinHandle<TunnelReport>>,
    pub completion: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl Outcome {
    pub fn origin_status(&self) -> Option<u16> {
        self.response
            .extensions()
            .get::<policy::OriginResult>()
            .map_or_else(
                || {
                    self.is_origin_response()
                        .then(|| self.response.status().as_u16())
                },
                |result| result.0,
            )
    }
    pub fn origin_transport_failed(&self) -> bool {
        self.response
            .extensions()
            .get::<policy::OriginResult>()
            .is_some_and(|result| result.0.is_none())
    }
    pub fn is_origin_response(&self) -> bool {
        self.response
            .extensions()
            .get::<policy::CacheResponse>()
            .is_none()
    }
}
pub struct TunnelReport {
    pub bytes: Option<u64>,
    pub error: bool,
}
#[derive(Clone, Copy, Default)]
pub struct ForwardOptions {
    pub request_policy: RequestPolicy,
    pub notify_completion: bool,
}
#[derive(Clone)]
pub struct Target {
    scheme: http::uri::Scheme,
    authority: http::uri::Authority,
}
impl Target {
    pub fn new(endpoint: &str, tls: bool) -> Result<Self> {
        Ok(Self {
            scheme: if tls {
                http::uri::Scheme::HTTPS
            } else {
                http::uri::Scheme::HTTP
            },
            authority: endpoint.parse()?,
        })
    }
}
pub async fn forward(
    request: Request<Incoming>,
    target: &Target,
    upstream: &Upstream,
    client: &HttpClient,
    permit: OwnedSemaphorePermit,
    keepalive: KeepAlive,
    options: ForwardOptions,
) -> Result<Outcome> {
    forward_inner(
        request.map(Some),
        target,
        upstream,
        client,
        Some(permit),
        keepalive,
        options,
    )
    .await
}
async fn forward_inner(
    mut request: Request<Option<Incoming>>,
    target: &Target,
    upstream: &Upstream,
    client: &HttpClient,
    permit: Option<OwnedSemaphorePermit>,
    keepalive: KeepAlive,
    options: ForwardOptions,
) -> Result<Outcome> {
    let (done, completion) = if options.notify_completion {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    ensure!(
        request.method() != http::Method::CONNECT,
        "CONNECT unsupported"
    );
    let upgrade = request.version() == http::Version::HTTP_11
        && request
            .headers()
            .get(header::UPGRADE)
            .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let downstream_upgrade = if upgrade {
        Some(hyper::upgrade::on(&mut request))
    } else {
        None
    };
    // Hyper's HTTP/1 encoder only sends trailer fields declared up front.
    // Rebuild this framing metadata after removing hop-by-hop headers.
    let trailers = (!upstream.http2 && !upgrade)
        .then(|| trailer_declaration(request.headers()))
        .flatten();
    sanitize(request.headers_mut(), upgrade, true);
    if let Some(trailers) = trailers {
        request.headers_mut().insert(header::TRAILER, trailers);
    }
    let mut parts = http::uri::Parts::default();
    parts.scheme = Some(target.scheme.clone());
    parts.authority = Some(target.authority.clone());
    parts.path_and_query = Some(
        request
            .uri()
            .path_and_query()
            .cloned()
            .unwrap_or_else(|| http::uri::PathAndQuery::from_static("/")),
    );
    *request.uri_mut() = Uri::from_parts(parts)?;
    *request.version_mut() = if upstream.http2 {
        http::Version::HTTP_2
    } else {
        http::Version::HTTP_11
    };
    if let Some(body) = request.body() {
        options.request_policy.check(body)?;
    }
    let (parts, incoming) = request.into_parts();
    let (body, upload) = match incoming {
        Some(body) => RequestBody::new(
            body,
            options.request_policy,
            upstream.response_header_timeout_ms.is_some_and(|ms| ms > 0),
        ),
        None => (RequestBody::Empty, None),
    };
    let mut request = Request::from_parts(parts, body);
    let captured = upstream
        .observe_pool_state
        .then(|| hyper_util::client::legacy::connect::capture_connection(&mut request));
    let mut response = if let Some(capture) = captured {
        Box::pin(await_headers(
            capture::Captured::new(client.request(request), capture),
            upstream,
            upload.as_ref(),
        ))
        .await?
    } else if upload.is_none() && upstream.response_header_timeout_ms == Some(0) {
        client.request(request).await?
    } else if upload.is_none() && upstream.response_header_timeout_ms.is_none() {
        // Keep the unconfigured path free of upload state and select machinery.
        tokio::time::timeout(
            Duration::from_millis(upstream.idle_timeout_ms),
            client.request(request),
        )
        .await??
    } else {
        // Keep optional policy state out of the common forwarding future.
        // One configured-path allocation also holds its inline header timer.
        Box::pin(await_headers(
            client.request(request),
            upstream,
            upload.as_ref(),
        ))
        .await?
    };
    if upstream.observe_connection_reuse {
        if let Some(tag) = response.extensions().get::<pool_stats::ConnectionTag>() {
            tag.response();
        }
    }
    let keepalive = match response
        .extensions_mut()
        .remove::<Arc<pool_stats::Activity>>()
    {
        Some(activity) => keepalive.with_activity(activity),
        None => keepalive,
    };
    let is_upgrade = upgrade && response.status() == http::StatusCode::SWITCHING_PROTOCOLS;
    sanitize(response.headers_mut(), is_upgrade, false);
    if is_upgrade {
        let server = hyper::upgrade::on(&mut response);
        let downstream = downstream_upgrade.unwrap();
        let idle = Duration::from_millis(upstream.idle_timeout_ms);
        let half_close = upstream
            .half_close_timeout_ms
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        let tunnel = tokio::spawn(async move {
            let _completion = Completion(done);
            let (_permit, _keepalive) = (permit, keepalive);
            if let (Ok(a), Ok(b)) = tokio::join!(downstream, server) {
                let result = prost_transport::relay_with_timeouts(
                    Box::new(TokioIo::new(a)),
                    Box::new(TokioIo::new(b)),
                    idle,
                    half_close,
                )
                .await;
                return match result {
                    Ok(bytes) => TunnelReport {
                        bytes: Some(bytes),
                        error: false,
                    },
                    Err(_) => TunnelReport {
                        bytes: None,
                        error: true,
                    },
                };
            }
            TunnelReport {
                bytes: None,
                error: true,
            }
        });
        return Ok(Outcome {
            response: response.map(Body::incoming),
            tunnel: Some(tunnel),
            completion,
        });
    }
    let (parts, mut incoming) = response.into_parts();
    // One non-blocking poll only: never wait for a streaming producer. When a
    // small HTTP/1 body already arrived with its headers, return its Bytes
    // directly and let Hyper coalesce headers/data in the downstream write.
    let prefix = if parts.version != http::Version::HTTP_2
        && incoming
            .size_hint()
            .exact()
            .is_some_and(|n| n > 0 && n <= 16384)
    {
        std::future::poll_fn(
            |cx| match std::pin::Pin::new(&mut incoming).poll_frame(cx) {
                std::task::Poll::Ready(Some(frame)) => std::task::Poll::Ready(frame.map(Some)),
                std::task::Poll::Ready(None) | std::task::Poll::Pending => {
                    std::task::Poll::Ready(Ok(None))
                }
            },
        )
        .await?
    } else {
        None
    };
    let guard = Guard {
        permit,
        keepalive: Some(keepalive),
        completion: Completion(done),
    };
    let body = if incoming.is_end_stream() && prefix.as_ref().is_some_and(|frame| frame.is_data()) {
        Body::buffered(
            prefix.unwrap().into_data().expect("data frame checked"),
            guard,
        )
    } else {
        Body::stream(
            incoming,
            prefix,
            Duration::from_millis(
                upstream
                    .response_body_timeout_ms
                    .unwrap_or(upstream.idle_timeout_ms),
            ),
            guard,
        )
    };
    let body = match upload.filter(|state| !state.complete()) {
        Some(state) => body.with_upload(upload::UploadWatch(state)),
        None => body,
    };
    Ok(Outcome {
        response: Response::from_parts(parts, body),
        tunnel: None,
        completion,
    })
}

fn trailer_declaration(headers: &HeaderMap) -> Option<HeaderValue> {
    let mut names = String::new();
    for name in headers
        .get_all(header::TRAILER)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|v| HeaderName::from_bytes(v.trim().as_bytes()).ok())
    {
        if FORWARDED_HOP_HEADERS.contains(&name)
            || matches!(
                name.as_str(),
                "content-length"
                    | "host"
                    | "authorization"
                    | "cookie"
                    | "set-cookie"
                    | "content-encoding"
                    | "content-type"
                    | "content-range"
            )
        {
            continue;
        }
        if !names.is_empty() {
            names.push_str(", ");
        }
        names.push_str(name.as_str());
    }
    (!names.is_empty()).then(|| HeaderValue::from_str(&names).expect("validated header names"))
}

async fn await_headers(
    future: impl std::future::Future<
        Output = std::result::Result<Response<Incoming>, hyper_util::client::legacy::Error>,
    >,
    upstream: &Upstream,
    upload: Option<&Arc<upload::UploadState>>,
) -> Result<Response<Incoming>> {
    use tokio::time::Instant;
    let timer = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(timer);
    let mut armed = upstream.response_header_timeout_ms.is_none();
    if armed {
        timer
            .as_mut()
            .reset(Instant::now() + Duration::from_millis(upstream.idle_timeout_ms));
    }
    tokio::pin!(future);
    loop {
        if let Some(error) = upload.and_then(|s| s.failure()) {
            return Err(error.into());
        }
        if !armed {
            if let Some(ms) = upstream.response_header_timeout_ms.filter(|ms| *ms > 0) {
                let ended = match upload {
                    Some(state) => state.ended(),
                    None => Some(Instant::now()),
                };
                if let Some(at) = ended {
                    timer.as_mut().reset(at + Duration::from_millis(ms));
                    armed = true;
                }
            }
        }
        tokio::select! {
            biased;
            _ = async { if let Some(state) = upload { state.changed.notified().await } else { std::future::pending::<()>().await } } => {},
            result = &mut future => {
                if let Some(error) = upload.and_then(|s| s.failure()) { return Err(error.into()); }
                return Ok(result?);
            },
            _ = &mut timer, if armed => {
                if upstream.response_header_timeout_ms.is_some() { return Err(HttpFailure::ResponseHeaderTimeout.into()); }
                anyhow::bail!("upstream idle timeout before response headers");
            },
        }
    }
}

#[cfg(test)]
mod tests;
