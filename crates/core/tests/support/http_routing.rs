use super::*;
use hyper::body::Body as _;

fn route(value: serde_json::Value) -> Result<prost_config::Route> {
    Ok(serde_json::from_value(value)?)
}
async fn checked(
    response: Response<Incoming>,
    backend: &str,
    selected: &str,
    head: bool,
) -> Result<()> {
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-selected"], selected);
    assert_eq!(response.headers()["x-seen-plugin"], "mutated");
    let body = response.into_body().collect().await?.to_bytes();
    assert_eq!(body.as_ref(), if head { b"" } else { backend.as_bytes() });
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn method_header_routing_h1_h2_linear_indexed_hot_apply_and_restart() -> Result<()> {
    for padding in [0, 40] {
        let blue = http_server("blue").await;
        let green = http_server("green").await;
        let mut f = Fixture::new(Protocol::Http, blue.address);
        if padding > 0 {
            f.config.runtime.access_log = Some(prost_config::AccessLog {
                path: f.dir.path().join("routing.jsonl"),
                ..Default::default()
            });
        }
        let mut other = f.config.upstreams["main"].clone();
        other.endpoints[0].address = green.address.to_string();
        f.config.upstreams.insert("green".into(), other);
        f.config.listeners[0].plugins = vec![builtin(
            "headers",
            serde_json::json!({"request":{"set":{"x-canary":"green","x-test-plugin":"mutated"}},"response":{"set":{"x-selected":"default"}}}),
        )?];
        let definitions = serde_json::json!([
            {"priority":50,"methods":["POST"],"headers":[{"name":"x-region","prefix":"eu-"}],"upstream":"green","tag":"post"},
            {"priority":40,"host":"api.test","methods":["GET"],"headers":[{"name":"X-Canary","equals":"green","ignore_case":true}],"upstream":"green","tag":"canary"},
            {"priority":35,"methods":["GET"],"headers":[{"name":"x-tag","matches":"^tenant-[0-9]+$"}],"upstream":"green","tag":"regex"},
            {"priority":30,"path_prefix":"/api/special","methods":["DELETE"],"upstream":"green","tag":"delete"},
            {"priority":20,"methods":["PATCH"],"headers":[{"name":"x-special","present":true}],"upstream":"green","tag":"present"},
            {"priority":10,"path_prefix":"/tie","upstream":"main","tag":"first"},
            {"priority":10,"path_prefix":"/tie","upstream":"green","tag":"second"},
            {"methods":["HEAD"],"upstream":"green","tag":"head"},
            {"methods":["CUSTOM"],"headers":[{"name":"x-region","not_equals":"blocked"}],"upstream":"green","tag":"custom"}
        ]);
        for mut definition in definitions.as_array().unwrap().clone() {
            let tag = definition.as_object_mut().unwrap().remove("tag").unwrap();
            definition["plugins"] = serde_json::json!([builtin(
                "headers",
                serde_json::json!({"response":{"set":{"x-selected":tag}}})
            )?]);
            f.config.listeners[0].routes.push(route(definition)?);
        }
        for i in 0..padding {
            f.config.listeners[0].routes.push(route(serde_json::json!({"priority":1000,"path_prefix":format!("/unused/{i}/"),"upstream":"main"}))?);
        }
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let (mut h1, c1) = hyper::client::conn::http1::handshake(TokioIo::new(
            TcpStream::connect(f.address()).await?,
        ))
        .await?;
        let d1 = tokio::spawn(c1);
        let (mut h2, c2) = hyper::client::conn::http2::handshake(
            TokioExecutor::new(),
            TokioIo::new(TcpStream::connect(f.address()).await?),
        )
        .await?;
        let d2 = tokio::spawn(c2);
        for (method, path, host, headers, backend, selected) in [
            ("GET", "/", "api.test", vec![], "blue", "default"),
            (
                "GET",
                "/",
                "api.test",
                vec![("x-canary", "red"), ("x-canary", "gReEn")],
                "green",
                "canary",
            ),
            (
                "GET",
                "/",
                "other.test",
                vec![("x-canary", "green")],
                "blue",
                "default",
            ),
            (
                "POST",
                "/",
                "api.test",
                vec![("x-region", "eu-west")],
                "green",
                "post",
            ),
            (
                "GET",
                "/",
                "api.test",
                vec![("x-region", "eu-west")],
                "blue",
                "default",
            ),
            (
                "GET",
                "/",
                "api.test",
                vec![("x-tag", "tenant-123")],
                "green",
                "regex",
            ),
            (
                "DELETE",
                "/api/special/item",
                "api.test",
                vec![],
                "green",
                "delete",
            ),
            (
                "PATCH",
                "/",
                "api.test",
                vec![("x-special", "")],
                "green",
                "present",
            ),
            ("PATCH", "/", "api.test", vec![], "blue", "default"),
            ("GET", "/tie/item", "api.test", vec![], "blue", "first"),
            ("HEAD", "/", "api.test", vec![], "green", "head"),
            ("get", "/", "api.test", vec![], "blue", "default"),
            (
                "CUSTOM",
                "/",
                "api.test",
                vec![("x-region", "ok")],
                "green",
                "custom",
            ),
            ("CUSTOM", "/", "api.test", vec![], "blue", "default"),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", host);
            for (name, value) in &headers {
                request = request.header(*name, *value);
            }
            h1.ready().await?;
            checked(
                h1.send_request(request.body(Full::new(Bytes::new()))?)
                    .await?,
                backend,
                selected,
                method == "HEAD",
            )
            .await?;
            let mut request = Request::builder()
                .method(method)
                .uri(format!("http://{host}{path}"));
            for (name, value) in &headers {
                request = request.header(*name, *value);
            }
            checked(
                h2.send_request(request.body(Full::new(Bytes::new()))?)
                    .await?,
                backend,
                selected,
                method == "HEAD",
            )
            .await?;
        }
        let mut held = h2
            .send_request(
                Request::builder()
                    .uri("http://api.test/hold")
                    .header("x-canary", "green")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(
            held.body_mut().frame().await.unwrap()?.into_data().unwrap(),
            "green"
        );
        let mut next = p.snapshot().config.clone();
        next.listeners[0].routes[1].headers[0].equals = Some("blue".into());
        p.apply(next.encode()?, 0).await?;
        for (key, backend, selected) in [("green", "blue", "default"), ("blue", "green", "canary")]
        {
            h1.ready().await?;
            checked(
                h1.send_request(
                    Request::builder()
                        .uri("/")
                        .header("host", "api.test")
                        .header("x-canary", key)
                        .body(Full::new(Bytes::new()))?,
                )
                .await?,
                backend,
                selected,
                false,
            )
            .await?;
            checked(
                h2.send_request(
                    Request::builder()
                        .uri("http://api.test/")
                        .header("x-canary", key)
                        .body(Full::new(Bytes::new()))?,
                )
                .await?,
                backend,
                selected,
                false,
            )
            .await?;
        }
        assert!(!held.body().is_end_stream());
        let persisted = std::fs::read(&f.path)?;
        for pattern in ["(", "a{1000000}"] {
            let mut bad = p.snapshot().config.clone();
            bad.listeners[0].routes[2].headers[0].matches = Some(pattern.into());
            assert!(p.validate(bad.encode()?).await.is_err());
            assert!(p.apply(bad.encode()?, 1).await.is_err());
        }
        let mut excessive = p.snapshot().config.clone();
        for _ in 0..17 {
            excessive.listeners[0].routes.push(route(serde_json::json!({"upstream":"main","headers":vec![serde_json::json!({"name":"x","matches":"a"});16]}))?);
        }
        assert!(p.apply(excessive.encode()?, 1).await.is_err());
        assert_eq!(p.snapshot().config.generation, 1);
        assert_eq!(std::fs::read(&f.path)?, persisted);
        drop(held);
        drop(h1);
        drop(h2);
        d1.abort();
        d2.abort();
        p.shutdown().await;
        drop(p);
        let restarted = f.start().await;
        let (mut h1, connection) = hyper::client::conn::http1::handshake(TokioIo::new(
            TcpStream::connect(f.address()).await?,
        ))
        .await?;
        let driver = tokio::spawn(connection);
        h1.ready().await?;
        checked(
            h1.send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "api.test")
                    .header("x-canary", "blue")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?,
            "green",
            "canary",
            false,
        )
        .await?;
        assert_eq!(restarted.snapshot().config.generation, 1);
        drop(h1);
        driver.abort();
        restarted.shutdown().await;
    }
    Ok(())
}

async fn upload_backend(label: &'static str, seen: Arc<tokio::sync::Notify>) -> Result<Server> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = service_fn(move |mut request: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let mut bytes = 0;
                        while let Some(frame) = request.body_mut().frame().await {
                            let Ok(frame) = frame else {
                                return Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(400)
                                        .body(Full::new(Bytes::new()))
                                        .unwrap(),
                                );
                            };
                            if let Some(data) = frame.data_ref() {
                                bytes += data.len();
                                seen.notify_one();
                            }
                        }
                        Ok(Response::new(Full::new(Bytes::from(format!(
                            "{label}:{bytes}"
                        )))))
                    }
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    Ok(Server { address, task })
}
fn upload(
    rx: tokio::sync::mpsc::UnboundedReceiver<Frame<Bytes>>,
) -> Request<http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>> {
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|frame| (Ok::<_, Infallible>(frame), rx))
    });
    Request::builder()
        .method("POST")
        .uri("http://test/upload")
        .header("x-upload", "yes")
        .body(StreamBody::new(stream).boxed_unsync())
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streaming_upload_pins_method_header_route_and_body_limit_during_apply() -> Result<()> {
    let seen = Arc::new(tokio::sync::Notify::new());
    let blue = upload_backend("blue", seen.clone()).await?;
    let green = upload_backend("green", Arc::new(tokio::sync::Notify::new())).await?;
    let mut f = Fixture::new(Protocol::Http, blue.address);
    let mut other = f.config.upstreams["main"].clone();
    other.endpoints[0].address = green.address.to_string();
    f.config.upstreams.insert("green".into(), other);
    f.config.listeners[0].routes = vec![route(
        serde_json::json!({"path_prefix":"/upload","methods":["POST"],"headers":[{"name":"x-upload","equals":"yes"}],"upstream":"main","max_request_body_bytes":32}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut sender = client.clone();
    let pending = tokio::spawn(async move { sender.send_request(upload(rx)).await });
    tx.send(Frame::data(Bytes::from_static(b"before")))?;
    timeout(Duration::from_secs(3), seen.notified()).await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].routes[0].upstream = "green".into();
    next.listeners[0].routes[0].max_request_body_bytes = Some(2);
    p.apply(next.encode()?, 0).await?;
    tx.send(Frame::data(Bytes::from_static(b"after")))?;
    drop(tx);
    let response = timeout(Duration::from_secs(3), pending).await???;
    assert_eq!(response.status(), 200);
    assert_eq!(response.into_body().collect().await?.to_bytes(), "blue:11");
    for (bytes, status, expected) in [
        (b"abc".as_slice(), 413, None),
        (b"ok".as_slice(), 200, Some("green:2")),
    ] {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(Frame::data(Bytes::copy_from_slice(bytes)))?;
        drop(tx);
        let response = client.send_request(upload(rx)).await?;
        assert_eq!(response.status(), status);
        let body = response.into_body().collect().await?.to_bytes();
        if let Some(expected) = expected {
            assert_eq!(body, expected);
        }
    }
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
