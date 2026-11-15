use super::*;
use prost_core::{cache_key, CacheKeyInput, CacheQuery};
use std::sync::Mutex;

struct Backend {
    server: Server,
    headers_started: Arc<tokio::sync::Notify>,
    headers_gate: Arc<tokio::sync::Notify>,
    body_gate: Arc<tokio::sync::Notify>,
}
async fn backend() -> Result<Backend> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let calls = Arc::new(Mutex::new(std::collections::HashMap::<String, usize>::new()));
    let headers_started = Arc::new(tokio::sync::Notify::new());
    let headers_gate = Arc::new(tokio::sync::Notify::new());
    let body_gate = Arc::new(tokio::sync::Notify::new());
    let task = {
        let headers_started = headers_started.clone();
        let headers_gate = headers_gate.clone();
        let body_gate = body_gate.clone();
        tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                let calls = calls.clone();
                let started = headers_started.clone();
                let headers_gate = headers_gate.clone();
                let body_gate = body_gate.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let calls = calls.clone();
                        let started = started.clone();
                        let headers_gate = headers_gate.clone();
                        let body_gate = body_gate.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let n = {
                                let mut calls = calls.lock().unwrap();
                                let n = calls.entry(path.clone()).or_default();
                                *n += 1;
                                *n
                            };
                            if path == "/headers" && n == 1 {
                                started.notify_one();
                                headers_gate.notified().await;
                            }
                            let body: http_body_util::combinators::UnsyncBoxBody<
                                Bytes,
                                Infallible,
                            > = if path == "/body" && n == 1 {
                                let first = futures_util::stream::once(async {
                                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"start")))
                                });
                                let last = futures_util::stream::once(async move {
                                    body_gate.notified().await;
                                    Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"end")))
                                });
                                StreamBody::new(first.chain(last)).boxed_unsync()
                            } else {
                                Full::new(Bytes::from(format!("{path}:{n}"))).boxed_unsync()
                            };
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .header("cache-control", "public,max-age=60")
                                    .header("x-count", n.to_string())
                                    .body(body)
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
        headers_started,
        headers_gate,
        body_gate,
    })
}
fn configure(f: &mut Fixture) {
    let upstream = f.config.upstreams.get_mut("main").unwrap();
    upstream.idle_timeout_ms = 60000;
    upstream.http = Some(prost_config::HttpPolicy {
        cache: Some(prost_config::Cache {
            max_bytes: 65536,
            max_entries: 16,
            max_object_bytes: 4096,
            ..Default::default()
        }),
        ..Default::default()
    });
}
fn key(uri: &str, host: Option<&str>, encoding: Option<&str>) -> String {
    cache_key(&CacheKeyInput {
        uri: uri.into(),
        host: host.map(str::to_owned),
        accept_encoding: encoding.map(|s| vec![s.into()]).unwrap_or_default(),
    })
    .unwrap()
}
fn request(path: &str, encoding: Option<&str>) -> Request<Full<Bytes>> {
    let mut request = Request::builder().uri(path).header("host", "cache.test");
    if let Some(encoding) = encoding {
        request = request.header("accept-encoding", encoding);
    }
    request.body(Full::new(Bytes::new())).unwrap()
}
async fn count(response: Response<Incoming>, expected: usize) -> Result<Bytes> {
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-count"], expected.to_string());
    Ok(response.into_body().collect().await?.to_bytes())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_and_all_purge_keep_variants_compatible_apply_and_restart_semantics() -> Result<()> {
    let backend = backend().await?;
    let mut f = Fixture::new(Protocol::Http, backend.server.address);
    configure(&mut f);
    let mut disabled = f.config.upstreams["main"].clone();
    disabled.http = None;
    f.config.upstreams.insert("disabled".into(), disabled);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, connection) =
        crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
    let driver = tokio::spawn(connection);
    count(client.send_request(request("/a", None)).await?, 1).await?;
    count(client.send_request(request("/a", None)).await?, 1).await?;
    count(client.send_request(request("/a", Some("gzip"))).await?, 2).await?;
    count(client.send_request(request("/b", None)).await?, 1).await?;
    let a = key("/a", Some("cache.test"), None);
    let report = p
        .cache_entries(
            "main".into(),
            CacheQuery {
                limit: 1,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(report["cache"]["status"]["entries"], 3);
    assert_eq!(report["cache"]["truncated"], true);
    assert_eq!(
        p.cache_entries(
            "main".into(),
            CacheQuery {
                key: Some(a.to_uppercase()),
                limit: 1
            }
        )
        .await?["cache"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        p.cache_status(None).await?["caches"]["disabled"]["enabled"],
        false
    );
    assert!(p
        .cache_entries("disabled".into(), CacheQuery::default())
        .await
        .is_err());
    for (key, all, generation) in [
        (None, false, 0),
        (Some(a.clone()), true, 0),
        (Some("bad".into()), false, 0),
        (Some(a.clone()), false, 9),
    ] {
        assert!(p
            .cache_purge("main".into(), key, all, generation)
            .await
            .is_err());
    }
    let persisted = std::fs::read(&f.path)?;
    let purged = p
        .cache_purge("main".into(), Some(a.clone()), false, 0)
        .await?;
    assert_eq!(purged["purge"]["removed_entries"], 1);
    assert_eq!(purged["purge"]["epoch"], 1);
    assert_eq!(std::fs::read(&f.path)?, persisted);
    count(client.send_request(request("/a", None)).await?, 3).await?;
    count(client.send_request(request("/a", Some("gzip"))).await?, 2).await?;
    count(client.send_request(request("/b", None)).await?, 1).await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins = vec![builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-new":"yes"}}}),
    )?];
    p.apply(next.encode()?, 0).await?;
    assert_eq!(
        p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]["epoch"],
        1
    );
    count(client.send_request(request("/a", None)).await?, 3).await?;
    let committed = std::fs::read(&f.path)?;
    assert_eq!(
        p.cache_purge("main".into(), None, true, 1).await?["purge"]["removed_entries"],
        3
    );
    let only = request("/a", None);
    let (mut parts, body) = only.into_parts();
    parts
        .headers
        .insert("cache-control", "only-if-cached".parse()?);
    let response = client
        .send_request(Request::from_parts(parts, body))
        .await?;
    assert_eq!(response.status(), 504);
    response.into_body().collect().await?;
    count(client.send_request(request("/a", None)).await?, 4).await?;
    assert_eq!(std::fs::read(&f.path)?, committed);
    assert_eq!(p.snapshot().config.generation, 1);
    drop(client);
    driver.abort();
    p.shutdown().await;
    drop(p);
    let restarted = f.start().await;
    assert_eq!(restarted.snapshot().config.generation, 1);
    let status = restarted.cache_status(Some("main".into())).await?;
    assert_eq!(status["caches"]["main"]["status"]["entries"], 0);
    assert_eq!(status["caches"]["main"]["status"]["epoch"], 0);
    let (mut client, connection) =
        crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
    let driver = tokio::spawn(connection);
    count(client.send_request(request("/a", None)).await?, 5).await?;
    drop(client);
    driver.abort();
    restarted.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn purge_fences_header_pending_and_active_body_without_resetting_h2_streams() -> Result<()> {
    let backend = backend().await?;
    let mut f = Fixture::new(Protocol::Http, backend.server.address);
    configure(&mut f);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    let request = |path: &str| {
        Request::builder()
            .uri(format!("http://cache.test{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let mut sender = client.clone();
    let pending = tokio::spawn(async move { sender.send_request(request("/headers")).await });
    timeout(Duration::from_secs(3), backend.headers_started.notified()).await?;
    let key = key("http://cache.test/headers", None, None);
    let purged = p.cache_purge("main".into(), Some(key), false, 0).await?;
    assert_eq!(purged["purge"]["removed_entries"], 0);
    backend.headers_gate.notify_one();
    count(timeout(Duration::from_secs(3), pending).await???, 1).await?;
    assert_eq!(
        p.cache_status(Some("main".into())).await?["caches"]["main"]["status"]["entries"],
        0
    );
    count(client.send_request(request("/headers")).await?, 2).await?;
    count(client.send_request(request("/headers")).await?, 2).await?;
    let mut held = client.send_request(request("/body")).await?;
    assert_eq!(
        held.body_mut().frame().await.unwrap()?.into_data().unwrap(),
        "start"
    );
    let purged = p.cache_purge("main".into(), None, true, 0).await?;
    assert_eq!(purged["purge"]["active_captures"], 1);
    backend.body_gate.notify_one();
    assert_eq!(held.into_body().collect().await?.to_bytes(), "end");
    let status = p.cache_status(Some("main".into())).await?;
    assert_eq!(status["caches"]["main"]["status"]["entries"], 0);
    assert_eq!(status["caches"]["main"]["status"]["used_bytes"], 0);
    assert!(
        status["caches"]["main"]["status"]["discarded_fills"]
            .as_u64()
            .unwrap()
            >= 2
    );
    count(client.send_request(request("/body")).await?, 2).await?;
    count(client.send_request(request("/body")).await?, 2).await?;
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
