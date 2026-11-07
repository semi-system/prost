use anyhow::{ensure, Result};
use bytes::Bytes;
use http::{header, HeaderMap, HeaderName, HeaderValue, Request, Response, Uri};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::TokioIo,
};
use prost_config::{Route, Upstream};
use std::{sync::Arc, time::Duration};
use tokio::sync::OwnedSemaphorePermit;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Body = UnsyncBoxBody<Bytes, Error>;
pub type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Incoming>;

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
        builder
            .with_server_name_resolver(move |_: &Uri| Ok::<_, Error>(name.clone()))
            .enable_http1()
            .enable_http2()
            .wrap_connector(connector)
    } else {
        builder
            .enable_http1()
            .enable_http2()
            .wrap_connector(connector)
    };
    let mut builder = Client::builder(hyper_util::rt::TokioExecutor::new());
    builder
        .pool_max_idle_per_host(16)
        .pool_idle_timeout(Duration::from_secs(30))
        .retry_canceled_requests(false);
    if upstream.http2 {
        builder.http2_only(true);
    }
    Ok(builder.build(connector))
}
pub fn boxed(body: Incoming) -> Body {
    body.map_err(|e| Box::new(e) as Error).boxed_unsync()
}
pub fn response(status: u16, text: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(
            Full::new(Bytes::from_static(text.as_bytes()))
                .map_err(|e| match e {})
                .boxed_unsync(),
        )
        .unwrap()
}

/// Removes nominated connection headers and all hop-by-hop headers. TE trailers is allowed for gRPC requests.
pub fn sanitize(headers: &mut HeaderMap, upgrade: bool, request: bool) {
    let nominated: Vec<HeaderName> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|s| s.split(','))
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
    for name in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        if upgrade && name == "upgrade" {
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
    pub completion: tokio::sync::oneshot::Receiver<()>,
}
struct Completion(Option<tokio::sync::oneshot::Sender<()>>);
impl Completion {
    fn finish(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finish();
    }
}
pub async fn forward(
    mut request: Request<Incoming>,
    endpoint: &str,
    upstream: &Upstream,
    client: &HttpClient,
    permits: Vec<OwnedSemaphorePermit>,
    keepalive: Arc<dyn Send + Sync>,
) -> Result<Outcome> {
    let (done, completion) = tokio::sync::oneshot::channel();
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
    let path = request
        .uri()
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let uri: Uri = format!(
        "{}://{}{}",
        if upstream.tls.is_some() {
            "https"
        } else {
            "http"
        },
        endpoint,
        path
    )
    .parse()?;
    *request.uri_mut() = uri;
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
            let _completion = Completion(Some(done));
            let (_permits, _keepalive) = (permits, keepalive);
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
            response: response.map(boxed),
            tunnel: Some(tunnel),
            completion,
        });
    }
    let (parts, incoming) = response.into_parts();
    Ok(Outcome {
        response: Response::from_parts(
            parts,
            GuardBody {
                inner: boxed(incoming),
                _permits: permits,
                _keepalive: keepalive,
                completion: Completion(Some(done)),
                idle: Box::pin(tokio::time::sleep(Duration::from_millis(
                    upstream.idle_timeout_ms,
                ))),
                timeout: Duration::from_millis(upstream.idle_timeout_ms),
            }
            .boxed_unsync(),
        ),
        tunnel: None,
        completion,
    })
}

struct GuardBody {
    inner: Body,
    _permits: Vec<OwnedSemaphorePermit>,
    _keepalive: Arc<dyn Send + Sync>,
    idle: std::pin::Pin<Box<tokio::time::Sleep>>,
    timeout: Duration,
    completion: Completion,
}
impl hyper::body::Body for GuardBody {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Error>>> {
        use std::{future::Future, task::Poll};
        match std::pin::Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(frame) => {
                if frame.is_none() || matches!(&frame, Some(Err(_))) {
                    self.completion.finish();
                }
                let timeout = self.timeout;
                self.idle
                    .as_mut()
                    .reset(tokio::time::Instant::now() + timeout);
                Poll::Ready(frame)
            }
            Poll::Pending => {
                if self.idle.as_mut().poll(cx).is_ready() {
                    Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "body idle timeout",
                    )
                    .into())))
                } else {
                    Poll::Pending
                }
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}
