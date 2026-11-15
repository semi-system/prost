use super::*;
use std::sync::{
    atomic::{AtomicU16, AtomicUsize, Ordering},
    Mutex,
};
type SeenRequests = Arc<Mutex<Vec<(String, Option<String>, Option<String>)>>>;
struct Backend {
    server: Server,
    status: Arc<AtomicU16>,
    version: Arc<AtomicUsize>,
    requests: SeenRequests,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
async fn backend() -> Result<Backend> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let status = Arc::new(AtomicU16::new(200));
    let version = Arc::new(AtomicUsize::new(1));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task = {
        let status = status.clone();
        let version = version.clone();
        let requests = requests.clone();
        let entered = entered.clone();
        let release = release.clone();
        tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                let status = status.clone();
                let version = version.clone();
                let requests = requests.clone();
                let entered = entered.clone();
                let release = release.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let status = status.clone();
                        let version = version.clone();
                        let requests = requests.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let tag = request
                                .headers()
                                .get("if-none-match")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            let modified = request
                                .headers()
                                .get("if-modified-since")
                                .and_then(|v| v.to_str().ok())
                                .map(str::to_owned);
                            requests.lock().unwrap().push((
                                path.clone(),
                                tag.clone(),
                                modified.clone(),
                            ));
                            let version = version.load(Ordering::Relaxed);
                            let current = format!("\"v{version}\"");
                            let mut code = status.load(Ordering::Relaxed);
                            if code == 0 {
                                return Err(std::io::Error::other("fixture disconnect"));
                            }
                            if code == 200
                                && (tag.as_deref() == Some(&current)
                                    || path == "/lm" && modified.is_some())
                            {
                                code = 304;
                            }
                            if path == "/gated" && code == 304 {
                                entered.notify_one();
                                release.notified().await;
                            }
                            if path == "/swr" && version == 2 {
                                entered.notify_one();
                                release.notified().await;
                            }
                            let control = if path == "/strict" {
                                "public,max-age=2,must-revalidate,stale-if-error=10"
                            } else if path == "/private" && version > 1
                                || path == "/swr" && version > 2
                            {
                                "private,no-store"
                            } else if path == "/swr" {
                                "public,max-age=1,stale-while-revalidate=10"
                            } else {
                                "public,max-age=2,stale-if-error=10"
                            };
                            let mut response = Response::builder()
                                .status(code)
                                .header("cache-control", control)
                                .header("x-validation", if code == 304 { "yes" } else { "no" });
                            response = if path == "/lm" {
                                response.header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                            } else {
                                response.header("etag", current)
                            };
                            Ok::<_, std::io::Error>(
                                response
                                    .body(Full::new(if code == 304 {
                                        Bytes::new()
                                    } else {
                                        Bytes::from(format!("body-{version}"))
                                    }))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                });
            }
        })
    };
    Ok(Backend {
        server: Server { address, task },
        status,
        version,
        requests,
        entered,
        release,
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_h2_swr_serves_without_waiting_singleflight_purge_and_private_replacement() -> Result<()>
{
    for h2 in [false, true] {
        let backend = backend().await?;
        let mut f = Fixture::new(Protocol::Http, backend.server.address);
        config(&mut f);
        let upstream = f.config.upstreams.get_mut("main").unwrap();
        upstream.http2 = h2;
        let cache = upstream.http.as_mut().unwrap().cache.as_mut().unwrap();
        cache.stale_while_revalidate_seconds = 10;
        cache.max_background_refreshes = 1;
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let client = client(h2);
        body(&client, f.address(), "/swr", 200, "body-1").await?;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        backend.version.store(2, Ordering::Relaxed);
        timeout(
            Duration::from_secs(1),
            body(&client, f.address(), "/swr", 200, "body-1"),
        )
        .await??;
        timeout(Duration::from_secs(3), backend.entered.notified()).await?;
        for _ in 0..5 {
            body(&client, f.address(), "/swr", 200, "body-1").await?;
        }
        assert_eq!(
            backend.requests.lock().unwrap().len(),
            2,
            "only one background refresh per key"
        );
        p.cache_purge("main".into(), None, true, 0).await?;
        backend.release.notify_one();
        for _ in 0..100 {
            if p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]
                ["background_refreshes"]
                == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]["entries"],
            0,
            "purge fences background publication"
        );
        backend.version.store(1, Ordering::Relaxed);
        body(&client, f.address(), "/swr", 200, "body-1").await?;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        backend.version.store(3, Ordering::Relaxed);
        body(&client, f.address(), "/swr", 200, "body-1").await?;
        for _ in 0..100 {
            let status = p.cache_status(Some("main".into())).await?;
            if status["caches"]["main"]["status"]["entries"] == 0
                && status["caches"]["main"]["status"]["background_refreshes"] == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        body(&client, f.address(), "/swr", 200, "body-3").await?;
        let calls = backend.requests.lock().unwrap().len();
        body(&client, f.address(), "/swr", 200, "body-3").await?;
        assert_eq!(
            backend.requests.lock().unwrap().len(),
            calls + 1,
            "private replacement cannot remain stale"
        );
        p.shutdown().await;
    }
    Ok(())
}
fn config(f: &mut Fixture) {
    f.config.upstreams.get_mut("main").unwrap().http = Some(prost_config::HttpPolicy {
        cache: Some(prost_config::Cache {
            revalidate: true,
            stale_if_error_seconds: 3,
            ..Default::default()
        }),
        ..Default::default()
    });
}
type Client = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    Full<Bytes>,
>;
fn client(h2: bool) -> Client {
    let mut builder = hyper_util::client::legacy::Client::builder(TokioExecutor::new());
    builder.http2_only(h2);
    builder.build_http()
}
fn request(address: SocketAddr, path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://{address}{path}"))
        .header("host", "cache.test")
        .body(Full::new(Bytes::new()))
        .unwrap()
}
async fn body(
    client: &Client,
    address: SocketAddr,
    path: &str,
    code: u16,
    expected: &str,
) -> Result<http::HeaderMap> {
    let response = timeout(
        Duration::from_secs(5),
        client.request(request(address, path)),
    )
    .await??;
    assert_eq!(response.status(), code);
    let headers = response.headers().clone();
    assert_eq!(response.into_body().collect().await?.to_bytes(), expected);
    Ok(headers)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_h2_revalidation_200_replacement_private_and_stale_errors() -> Result<()> {
    for h2 in [false, true] {
        let backend = backend().await?;
        let mut f = Fixture::new(Protocol::Http, backend.server.address);
        config(&mut f);
        f.config.upstreams.get_mut("main").unwrap().http2 = h2;
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let client = client(h2);
        for path in ["/etag", "/lm", "/private", "/strict"] {
            body(&client, f.address(), path, 200, "body-1").await?;
        }
        tokio::time::sleep(Duration::from_millis(2100)).await;
        for path in ["/etag", "/lm"] {
            assert_eq!(
                body(&client, f.address(), path, 200, "body-1").await?["x-validation"],
                "yes"
            );
            let calls = backend.requests.lock().unwrap().len();
            body(&client, f.address(), path, 200, "body-1").await?;
            assert_eq!(backend.requests.lock().unwrap().len(), calls);
        }
        assert!(backend
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(p, t, _)| p == "/etag" && t.as_deref() == Some("\"v1\"")));
        assert!(backend
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(p, _, m)| p == "/lm" && m.is_some()));
        backend.version.store(2, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        body(&client, f.address(), "/etag", 200, "body-2").await?;
        body(&client, f.address(), "/private", 200, "body-2").await?;
        let calls = backend.requests.lock().unwrap().len();
        body(&client, f.address(), "/private", 200, "body-2").await?;
        assert_eq!(backend.requests.lock().unwrap().len(), calls + 1);
        tokio::time::sleep(Duration::from_millis(2100)).await;
        for code in [500, 502, 503, 504] {
            backend.status.store(code, Ordering::Relaxed);
            body(&client, f.address(), "/etag", 200, "body-2").await?;
        }
        body(&client, f.address(), "/strict", 504, "body-2").await?;
        p.set_enabled("main/one".into(), false, 0).await?;
        body(&client, f.address(), "/etag", 200, "body-2").await?;
        let status = p.cache_status(Some("main".into())).await?;
        assert!(
            status["caches"]["main"]["status"]["revalidations"]
                .as_u64()
                .unwrap()
                >= 2
        );
        assert!(
            status["caches"]["main"]["status"]["stale_hits"]
                .as_u64()
                .unwrap()
                >= 5
        );
        p.shutdown().await;
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_racing_conditional_304_does_not_republish_old_body() -> Result<()> {
    let backend = backend().await?;
    let mut f = Fixture::new(Protocol::Http, backend.server.address);
    config(&mut f);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let client = client(true);
    body(&client, f.address(), "/gated", 200, "body-1").await?;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    let pending = tokio::spawn({
        let client = client.clone();
        let address = f.address();
        async move { body(&client, address, "/gated", 200, "body-1").await }
    });
    timeout(Duration::from_secs(3), backend.entered.notified()).await?;
    p.cache_purge("main".into(), None, true, 0).await?;
    backend.release.notify_one();
    pending.await??;
    assert_eq!(
        p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]["entries"],
        0
    );
    body(&client, f.address(), "/gated", 200, "body-1").await?;
    assert_eq!(backend.requests.lock().unwrap().len(), 3);
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_deadline_keeps_expiry_bounds() -> Result<()> {
    let backend = backend().await?;
    let mut f = Fixture::new(Protocol::Http, backend.server.address);
    config(&mut f);
    f.config
        .upstreams
        .get_mut("main")
        .unwrap()
        .http
        .as_mut()
        .unwrap()
        .cache
        .as_mut()
        .unwrap()
        .stale_if_error_seconds = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let client = client(false);
    body(&client, f.address(), "/bounded", 200, "body-1").await?;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    backend.status.store(503, Ordering::Relaxed);
    body(&client, f.address(), "/bounded", 200, "body-1").await?;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    body(&client, f.address(), "/bounded", 503, "body-1").await?;
    assert_eq!(
        p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]["stale_hits"],
        1
    );
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_fallback_preserves_origin_errors_and_passive_health() -> Result<()> {
    for status in [503, 0] {
        let backend = backend().await?;
        let mut f = Fixture::new(Protocol::Http, backend.server.address);
        config(&mut f);
        let upstream = f.config.upstreams.get_mut("main").unwrap();
        upstream.health_interval_ms = 60000;
        upstream.passive_health = Some(prost_config::PassiveHealth {
            failure_statuses: vec![503],
            ..Default::default()
        });
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let client = client(false);
        body(&client, f.address(), "/failure", 200, "body-1").await?;
        tokio::time::sleep(Duration::from_millis(2100)).await;
        backend.status.store(status, Ordering::Relaxed);
        body(&client, f.address(), "/failure", 200, "body-1").await?;
        assert_eq!(
            p.status()["upstreams"]["main"][0]["passive_health"]["ejected"],
            true
        );
        assert_eq!(
            metric(
                &p,
                "prost_upstream_passive_failures_total",
                &[("upstream", "main"), ("endpoint", "one")]
            ),
            Some(1.)
        );
        let calls = backend.requests.lock().unwrap().len();
        body(&client, f.address(), "/failure", 200, "body-1").await?;
        assert_eq!(backend.requests.lock().unwrap().len(), calls);
        p.shutdown().await;
    }
    Ok(())
}
