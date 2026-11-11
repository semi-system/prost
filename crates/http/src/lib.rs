use anyhow::{ensure, Result};
mod body;
pub use body::{Body, KeepAlive};
use body::{Completion, Guard};
use http::{header, HeaderMap, HeaderName, HeaderValue, Request, Response, Uri};
use hyper::body::Body as _;
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioIo,
};
use prost_config::{Route, Upstream};
use std::{sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Incoming>;
thread_local! { static WORKER_SLOT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
pub fn set_worker_slot(slot: usize) {
    WORKER_SLOT.with(|current| current.set(slot));
}
pub struct ClientPool {
    clients: Vec<HttpClient>,
}
impl ClientPool {
    pub fn new(
        upstream: &Upstream,
        tls: Arc<rustls::ClientConfig>,
        workers: usize,
    ) -> Result<Self> {
        ensure!(
            (1..=32).contains(&workers),
            "invalid client pool worker count"
        );
        let mut clients = Vec::with_capacity(workers);
        for index in 0..workers {
            let mut config = upstream.clone();
            config.pool_max_idle_per_host = upstream.pool_max_idle_per_host / workers
                + usize::from(index < upstream.pool_max_idle_per_host % workers);
            clients.push(client(&config, tls.clone())?);
        }
        Ok(Self { clients })
    }
    pub fn current(&self) -> &HttpClient {
        WORKER_SLOT.with(|slot| &self.clients[slot.get() % self.clients.len()])
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
    let mut connector = HttpConnector::new();
    connector.enforce_http(false);
    connector.set_connect_timeout(Some(Duration::from_millis(upstream.connect_timeout_ms)));
    connector.set_nodelay(true);
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
    Ok(builder.build(connector))
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
    pub tunnel: Option<tokio::task::JoinHandle<()>>,
    pub completion: Option<tokio::sync::oneshot::Receiver<()>>,
}
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
    mut request: Request<Incoming>,
    target: &Target,
    upstream: &Upstream,
    client: &HttpClient,
    permit: OwnedSemaphorePermit,
    keepalive: KeepAlive,
    notify_completion: bool,
) -> Result<Outcome> {
    let (done, completion) = if notify_completion {
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
    sanitize(request.headers_mut(), upgrade, true);
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
    let mut response = tokio::time::timeout(
        Duration::from_millis(upstream.idle_timeout_ms),
        client.request(request),
    )
    .await??;
    let is_upgrade = upgrade && response.status() == http::StatusCode::SWITCHING_PROTOCOLS;
    sanitize(response.headers_mut(), is_upgrade, false);
    if is_upgrade {
        let server = hyper::upgrade::on(&mut response);
        let downstream = downstream_upgrade.unwrap();
        let idle = Duration::from_millis(upstream.idle_timeout_ms);
        let tunnel = tokio::spawn(async move {
            let _completion = Completion(done);
            let (_permit, _keepalive) = (permit, keepalive);
            if let (Ok(a), Ok(b)) = tokio::join!(downstream, server) {
                let _ = prost_transport::relay(
                    Box::new(TokioIo::new(a)),
                    Box::new(TokioIo::new(b)),
                    idle,
                )
                .await;
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
        permit: Some(permit),
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
            Duration::from_millis(upstream.idle_timeout_ms),
            guard,
        )
    };
    Ok(Outcome {
        response: Response::from_parts(parts, body),
        tunnel: None,
        completion,
    })
}

#[cfg(test)]
mod tests;
