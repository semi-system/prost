use super::*;

async fn upload_backend() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            tokio::spawn(async move {
                let service = service_fn(|request: Request<Incoming>| async move {
                    let delayed = request.uri().path() == "/delayed";
                    let collected = request.into_body().collect().await;
                    if delayed {
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    let response = match collected {
                        Ok(body) => {
                            let trailer =
                                body.trailers().and_then(|t| t.get("x-upload-end")).cloned();
                            let mut response = Response::new(Full::new(body.to_bytes()));
                            if let Some(value) = trailer {
                                response.headers_mut().insert("x-seen-trailer", value);
                            }
                            response
                        }
                        Err(_) => Response::builder()
                            .status(400)
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    };
                    Ok::<_, Infallible>(response)
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    Server { address, task }
}
pub(super) async fn raw_response(address: SocketAddr, data: &[u8]) -> Result<String> {
    let mut io = TcpStream::connect(address).await?;
    io.write_all(data).await?;
    timeout(Duration::from_secs(3), async {
        let mut bytes = Vec::new();
        loop {
            let mut buf = [0; 1024];
            let n = io.read(&mut buf).await?;
            bytes.extend_from_slice(&buf[..n]);
            if n == 0 || bytes.windows(4).any(|p| p == b"\r\n\r\n") {
                break;
            }
        }
        Ok::<_, anyhow::Error>(String::from_utf8_lossy(&bytes).into_owned())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limits_h1_known_chunked_trailers_idle_and_passive_health() -> Result<()> {
    let backend = upload_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let l = &mut f.config.listeners[0];
    l.max_request_body_bytes = Some(4);
    l.request_body_timeout_ms = Some(100);
    l.plugins.push(builtin(
        "headers",
        serde_json::json!({"response":{"add":[{"name":"x-policy","value":"yes","always":true}]}}),
    )?);
    f.config.upstreams.get_mut("main").unwrap().passive_health =
        Some(prost_config::PassiveHealth::default());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let known = raw_response(
        f.address(),
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\n",
    )
    .await?;
    assert!(known.starts_with("HTTP/1.1 413"), "{known}");
    assert!(known.contains("x-policy: yes"));
    let labels = [("upstream", "main"), ("endpoint", "one")];
    assert_eq!(
        metric(&p, "prost_upstream_selections_total", &labels),
        Some(0.)
    );
    for (body, expected) in [
        ("3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n", 413),
        ("2\r\nab\r\n2\r\ncd\r\n0\r\nx-upload-end: yes\r\n\r\n", 200),
    ] {
        let data = format!("POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nTrailer: x-upload-end\r\nConnection: close\r\n\r\n{body}");
        let response = raw_response(f.address(), data.as_bytes()).await?;
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected}")),
            "{response}"
        );
        if expected == 200 {
            assert!(response.contains("x-seen-trailer: yes"), "{response}");
        }
    }
    let slow = raw_response(
        f.address(),
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\na",
    )
    .await?;
    assert!(slow.starts_with("HTTP/1.1 408"), "{slow}");
    let ok = raw_response(
        f.address(),
        b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabcd",
    )
    .await?;
    assert!(ok.starts_with("HTTP/1.1 200"), "{ok}");
    assert_eq!(
        metric(&p, "prost_upstream_forward_errors_total", &labels),
        Some(0.)
    );
    assert_eq!(p.status()["upstreams"]["main"][0]["healthy"], true);
    p.shutdown().await;
    Ok(())
}

fn upload_stream(
    rx: tokio::sync::mpsc::UnboundedReceiver<Frame<Bytes>>,
) -> StreamBody<
    impl futures_util::Stream<Item = std::result::Result<Frame<Bytes>, Infallible>> + Send,
> {
    StreamBody::new(futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|frame| (Ok(frame), rx))
    }))
}
fn upload_request(
    path: &str,
    rx: tokio::sync::mpsc::UnboundedReceiver<Frame<Bytes>>,
) -> Request<http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible>> {
    Request::builder()
        .method("POST")
        .uri(format!("http://localhost{path}"))
        .header("te", "trailers")
        .body(upload_stream(rx).boxed_unsync())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplex_h2_limit_after_response_headers_aborts_stream_and_releases_lease() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let backend = Server {
        address,
        task: tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let service = service_fn(|r: Request<Incoming>| async {
                        Ok::<_, Infallible>(Response::new(r.into_body()))
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                });
            }
        }),
    };
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].max_request_body_bytes = Some(4);
    f.config.listeners[0].plugins.push(builtin(
        "headers",
        serde_json::json!({"trailers":[{"name":"x-end","value":"ok","always":true}]}),
    )?);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.http2 = true;
    u.response_header_timeout_ms = Some(1000);
    u.endpoints[0].max_active = 1;
    u.passive_health = Some(prost_config::PassiveHealth::default());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let io = TcpStream::connect(f.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"abc")))?;
    let mut response = client.send_request(upload_request("/", rx)).await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .body_mut()
            .frame()
            .await
            .unwrap()?
            .into_data()
            .unwrap(),
        "abc"
    );
    tx.send(Frame::data(Bytes::from_static(b"de")))?;
    drop(tx);
    assert!(
        timeout(Duration::from_secs(2), response.into_body().collect())
            .await?
            .is_err()
    );
    wait_active(&p, &[0]).await?;
    assert_eq!(
        metric(
            &p,
            "prost_upstream_forward_errors_total",
            &[("upstream", "main"), ("endpoint", "one")]
        ),
        Some(0.)
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"ok")))?;
    drop(tx);
    let response = client.send_request(upload_request("/", rx)).await?;
    assert_eq!(response.status(), 200);
    let body = response.into_body().collect().await?;
    assert_eq!(body.trailers().unwrap()["x-end"], "ok");
    assert_eq!(body.to_bytes(), "ok");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limits_h2_hot_apply_retains_upload_policy_and_reuses_pool() -> Result<()> {
    let backend = upload_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].max_request_body_bytes = Some(8);
    f.config.listeners[0].request_body_timeout_ms = Some(2000);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.http2 = true;
    u.response_header_timeout_ms = Some(1000);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let old_generation = Arc::downgrade(&p.snapshot());
    let io = TcpStream::connect(f.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"abcd")))?;
    let held = client.send_request(upload_request("/", rx));
    let held = tokio::spawn(held);
    timeout(Duration::from_secs(2), async {
        while metric(
            &p,
            "prost_upstream_selections_total",
            &[("upstream", "main"), ("endpoint", "one")],
        ) != Some(1.)
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].max_request_body_bytes = Some(3);
    p.apply(next.encode()?, 0).await?;
    assert!(old_generation.upgrade().is_some());
    assert_eq!(
        metric(&p, "prost_snapshot_http_client_pools", &[]),
        Some(1.)
    );
    tx.send(Frame::data(Bytes::from_static(b"efgh")))?;
    drop(tx);
    let response = timeout(Duration::from_secs(2), held).await???;
    assert_eq!(response.status(), 200);
    assert_eq!(response.into_body().collect().await?.to_bytes(), "abcdefgh");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"abcd")))?;
    drop(tx);
    let response = client.send_request(upload_request("/", rx)).await?;
    assert_eq!(response.status(), 413);
    response.into_body().collect().await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].routes.push(prost_config::Route {
        methods: vec![],
        headers: vec![],
        access_log: None,
        host: None,
        path_prefix: "/unlimited".into(),
        priority: 0,
        upstream: "main".into(),
        plugins: vec![],
        max_request_body_bytes: Some(0),
        request_body_timeout_ms: Some(0),
    });
    p.apply(next.encode()?, 1).await?;
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"123456789")))?;
    drop(tx);
    let response = client
        .send_request(upload_request("/unlimited", rx))
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await?.to_bytes(),
        "123456789"
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    assert!(old_generation.upgrade().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_header_deadline_starts_after_upload_and_timeout_returns_504() -> Result<()> {
    let backend = upload_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.response_header_timeout_ms = Some(100);
    u.idle_timeout_ms = 50;
    f.config.listeners[0].request_body_timeout_ms = Some(1000);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let io = TcpStream::connect(f.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tx.send(Frame::data(Bytes::from_static(b"first")))?;
    let response = tokio::spawn(client.send_request(upload_request("/", rx)));
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!response.is_finished());
    tx.send(Frame::data(Bytes::from_static(b"last")))?;
    drop(tx);
    let response = timeout(Duration::from_secs(2), response).await???;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.into_body().collect().await?.to_bytes(),
        "firstlast"
    );
    let result = raw_response(
        f.address(),
        b"GET /delayed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await?;
    assert!(result.starts_with("HTTP/1.1 504"), "{result}");
    let mut next = p.snapshot().config.clone();
    next.upstreams
        .get_mut("main")
        .unwrap()
        .response_header_timeout_ms = Some(0);
    p.apply(next.encode()?, 0).await?;
    let result = raw_response(
        f.address(),
        b"GET /delayed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await?;
    assert!(result.starts_with("HTTP/1.1 200"), "{result}");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_body_timeout_aborts_stream_and_releases_admission() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.response_body_timeout_ms = Some(80);
    u.endpoints[0].max_active = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let io = TcpStream::connect(f.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let request = |path| {
        Request::builder()
            .uri(format!("http://localhost{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let mut response = client.send_request(request("/hold")).await?;
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .body_mut()
            .frame()
            .await
            .unwrap()?
            .into_data()
            .unwrap(),
        "data"
    );
    assert!(
        timeout(Duration::from_secs(2), response.into_body().collect())
            .await?
            .is_err()
    );
    wait_active(&p, &[0]).await?;
    assert_eq!(client.send_request(request("/")).await?.status(), 200);
    let mut next = p.snapshot().config.clone();
    next.upstreams
        .get_mut("main")
        .unwrap()
        .response_body_timeout_ms = Some(0);
    p.apply(next.encode()?, 0).await?;
    let mut response = client.send_request(request("/hold")).await?;
    response.body_mut().frame().await.unwrap()?;
    assert!(
        timeout(Duration::from_millis(160), response.body_mut().frame())
            .await
            .is_err()
    );
    drop(response);
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initial_header_deadline_covers_protocol_sniff_and_can_be_disabled() -> Result<()> {
    let backend = http_server("ok").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].header_read_timeout_ms = 80;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for prefix in [
        b"".as_slice(),
        b"GET / HTTP/1.1\r\nHost:",
        b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n",
    ] {
        let mut io = TcpStream::connect(f.address()).await?;
        io.write_all(prefix).await?;
        let mut buf = Vec::new();
        let _ = timeout(Duration::from_secs(2), io.read_to_end(&mut buf)).await?;
    }
    let mut next = p.snapshot().config.clone();
    next.listeners[0].header_read_timeout_ms = 0;
    p.apply(next.encode()?, 0).await?;
    let mut io = TcpStream::connect(f.address()).await?;
    tokio::time::sleep(Duration::from_millis(160)).await;
    io.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;
    let mut buf = Vec::new();
    timeout(Duration::from_secs(2), io.read_to_end(&mut buf)).await??;
    assert!(buf.starts_with(b"HTTP/1.1 200"));
    p.shutdown().await;
    Ok(())
}
