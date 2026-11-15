use anyhow::Result;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{
    body::{Bytes, Incoming},
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct State {
    pub tokens: RwLock<HashMap<(String, String), String>>,
    pub issued: AtomicU64,
    pub failures: AtomicU64,
    pub published: AtomicU64,
}
pub fn response(request: &Request<Incoming>, state: &State) -> Response<Full<Bytes>> {
    let path = request.uri().path();
    let host = request
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.parse::<http::uri::Authority>().ok())
        .map(|a| a.host().to_ascii_lowercase());
    let (status, body) = if request.method() != http::Method::GET
        && request.method() != http::Method::HEAD
    {
        (StatusCode::METHOD_NOT_ALLOWED, String::new())
    } else if path == "/healthz" {
        (StatusCode::OK, "ready\n".into())
    } else if path == "/metrics" {
        (StatusCode::OK,format!("# TYPE prost_acme_issued_total counter\nprost_acme_issued_total {}\n# TYPE prost_acme_failures_total counter\nprost_acme_failures_total {}\n# TYPE prost_acme_publications_total counter\nprost_acme_publications_total {}\n",state.issued.load(Ordering::Relaxed),state.failures.load(Ordering::Relaxed),state.published.load(Ordering::Relaxed)))
    } else if let (Some(token), Some(host)) =
        (path.strip_prefix("/.well-known/acme-challenge/"), host)
    {
        match state
            .tokens
            .read()
            .unwrap()
            .get(&(host, token.to_owned()))
            .cloned()
        {
            Some(value) => (StatusCode::OK, value),
            None => (StatusCode::NOT_FOUND, String::new()),
        }
    } else {
        (StatusCode::NOT_FOUND, String::new())
    };
    let mut response = Response::new(Full::new(Bytes::from(
        if request.method() == http::Method::HEAD {
            String::new()
        } else {
            body
        },
    )));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("cache-control", http::HeaderValue::from_static("no-store"));
    response.headers_mut().insert(
        "content-type",
        http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
}
pub async fn serve(listener: TcpListener, state: Arc<State>, stop: CancellationToken) {
    let capacity = Arc::new(Semaphore::new(32));
    let tasks = tokio_util::task::TaskTracker::new();
    loop {
        let accepted = tokio::select! {_=stop.cancelled()=>break,value=listener.accept()=>value};
        let Ok((io, _)) = accepted else {
            continue;
        };
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            continue;
        };
        let state = state.clone();
        tasks.spawn(async move {
            let _permit = permit;
            let service = service_fn(move |request| {
                let state = state.clone();
                async move { Ok::<_, Infallible>(response(&request, &state)) }
            });
            let mut builder = hyper::server::conn::http1::Builder::new();
            builder
                .timer(TokioTimer::new())
                .header_read_timeout(Duration::from_secs(10))
                .max_buf_size(16384)
                .max_headers(32);
            let _ = tokio::time::timeout(
                Duration::from_secs(15),
                builder.serve_connection(TokioIo::new(io), service),
            )
            .await;
        });
    }
    tasks.close();
    let _ = tokio::time::timeout(Duration::from_secs(16), tasks.wait()).await;
}

type Client = hyper_util::client::legacy::Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    instant_acme::BodyWrapper<Bytes>,
>;
pub struct BoundedClient(Client);
impl BoundedClient {
    pub fn new(ca: Option<&std::path::Path>) -> Result<Self> {
        let config = ca.map(|path| prost_config::ClientTls {
            policy: None,
            server_name: "acme".into(),
            ca_certificate: Some(path.to_owned()),
            identity: None,
        });
        let tls = prost_transport::client_tls(config.as_ref())?;
        let connector = hyper_rustls::HttpsConnectorBuilder::new()
            .with_tls_config((*tls).clone())
            .https_only()
            .enable_http1()
            .build();
        Ok(Self(
            hyper_util::client::legacy::Client::builder(TokioExecutor::new())
                .pool_max_idle_per_host(2)
                .build(connector),
        ))
    }
}
impl instant_acme::HttpClient for BoundedClient {
    fn request(
        &self,
        request: Request<instant_acme::BodyWrapper<Bytes>>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = std::result::Result<instant_acme::BytesResponse, instant_acme::Error>,
                > + Send,
        >,
    > {
        let future = self.0.request(request);
        Box::pin(async move {
            let response = tokio::time::timeout(Duration::from_secs(15), future)
                .await
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?;
            let (parts, body) = response.into_parts();
            let body = tokio::time::timeout(
                Duration::from_secs(15),
                Limited::new(body, 1024 * 1024).collect(),
            )
            .await
            .map_err(|e| instant_acme::Error::Other(Box::new(e)))?
            .map_err(instant_acme::Error::Other)?
            .to_bytes();
            Ok(instant_acme::BytesResponse {
                parts,
                body: Box::new(body),
            })
        })
    }
}
