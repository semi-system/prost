use super::*;
use serde_json::Value;

fn enable(f: &mut Fixture) -> Result<PathBuf> {
    let path = f.dir.path().join("access.jsonl");
    f.config.runtime.access_log = Some(prost_config::AccessLog {
        path: path.clone(),
        flush_interval_ms: 60000,
        ..Default::default()
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    Ok(path)
}
fn records(path: &std::path::Path) -> Result<Vec<Value>> {
    std::fs::read_to_string(path)?
        .lines()
        .map(|l| Ok(serde_json::from_str(l)?))
        .collect()
}
async fn wait_records(
    p: &Arc<RunningProxy>,
    path: &std::path::Path,
    count: usize,
) -> Result<Vec<Value>> {
    timeout(Duration::from_secs(3), async {
        loop {
            p.access_log_control(false, p.snapshot().config.generation)
                .await?;
            let found = records(path)?;
            if found.len() >= count {
                return Ok::<_, anyhow::Error>(found);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}
fn request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://localhost{path}"))
        .header("te", "trailers")
        .header("authorization", "secret-header")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_completion_trailers_bodyless_rotation_and_hot_flags() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.upstreams.get_mut("main").unwrap().http2 = true;
    let path = enable(&mut f)?;
    let p = f.start().await;
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    for uri in [
        "/?token=secret-query",
        "/grpc",
        "/status/204",
        "/status/304",
    ] {
        let body = client
            .send_request(request(uri))
            .await?
            .into_body()
            .collect()
            .await?;
        if uri == "/grpc" {
            assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
        }
    }
    let mut head = request("/");
    *head.method_mut() = http::Method::HEAD;
    assert!(client
        .send_request(head)
        .await?
        .into_body()
        .collect()
        .await?
        .to_bytes()
        .is_empty());
    let first = wait_records(&p, &path, 5).await?;
    assert_eq!(first.len(), 5);
    assert_eq!(first[0]["path"], "/");
    assert_eq!(first[0]["body_bytes"], 4);
    assert_eq!(first[0]["endpoint"], backend.address.to_string());
    assert_eq!(first[1]["body_bytes"], 9);
    for line in &first[2..] {
        assert_eq!(line["body_bytes"], 0);
    }
    let raw = std::fs::read_to_string(&path)?;
    assert!(!raw.contains("secret"));
    let mut held = client.send_request(request("/hold")).await?;
    assert_eq!(
        held.body_mut().frame().await.unwrap()?.into_data().unwrap(),
        "data"
    );
    p.access_log_control(false, 0).await?;
    assert_eq!(
        records(&path)?.len(),
        5,
        "open streams must not be logged as complete"
    );
    let mut next = p.snapshot().config.clone();
    next.listeners[0].access_log = false;
    next.listeners[0].routes.push(serde_json::from_value(
        serde_json::json!({"path_prefix":"/enabled","upstream":"main","access_log":true}),
    )?);
    p.apply(next.encode()?, 0).await?;
    client
        .send_request(request("/quiet"))
        .await?
        .into_body()
        .collect()
        .await?;
    client
        .send_request(request("/enabled"))
        .await?
        .into_body()
        .collect()
        .await?;
    drop(held);
    drop(client);
    driver.abort();
    let found = wait_records(&p, &path, 7).await?;
    assert_eq!(found.len(), 7);
    assert!(found
        .iter()
        .any(|l| l["path"] == "/hold" && l["generation"] == 0 && l["outcome"] == "aborted"));
    assert!(found
        .iter()
        .any(|l| l["path"] == "/enabled" && l["generation"] == 1));
    assert!(!found.iter().any(|l| l["path"] == "/quiet"));
    let mut invalid = p.snapshot().config.clone();
    invalid.runtime.access_log.as_mut().unwrap().sample_every = 2;
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    let rotated = f.dir.path().join("rotated.jsonl");
    std::fs::rename(&path, &rotated)?;
    assert_eq!(p.access_log_control(true, 1).await?["epoch"], 1);
    assert!(p.access_log_control(true, 0).await.is_err());
    let io = TcpStream::connect(f.address()).await?;
    let (mut h1, conn) = crate::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    h1.send_request(request("/enabled/h1"))
        .await?
        .into_body()
        .collect()
        .await?;
    let found = wait_records(&p, &path, 1).await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["http_version"], "HTTP/1.1");
    assert_eq!(records(&rotated)?.len(), 7);
    drop(h1);
    driver.abort();
    p.shutdown().await;
    assert_eq!(
        metric(&p, "prost_access_log_observations_active", &[]),
        Some(0.)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_timeout_is_logged_after_headers_and_local_rejections_keep_status() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config
        .upstreams
        .get_mut("main")
        .unwrap()
        .response_body_timeout_ms = Some(50);
    let path = enable(&mut f)?;
    let p = f.start().await;
    let (mut client, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(conn);
    let response = client.send_request(request("/slow")).await?;
    assert_eq!(response.status(), 200);
    assert!(response.into_body().collect().await.is_err());
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"deny":["127.0.0.0/8"]}),
    )?);
    p.apply(next.encode()?, 0).await?;
    let response = client.send_request(request("/denied")).await?;
    assert_eq!(response.status(), 403);
    response.into_body().collect().await?;
    let found = wait_records(&p, &path, 2).await?;
    assert_eq!(found[0]["outcome"], "error");
    assert_eq!(found[0]["status"], 200);
    assert_eq!(found[1]["status"], 403);
    assert_eq!(found[1]["outcome"], "complete");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tcp_half_close_logs_old_generation_at_session_end() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    let path = enable(&mut f)?;
    let p = f.start().await;
    let mut io = TcpStream::connect(f.address()).await?;
    roundtrip(&mut io, b"payload").await?;
    p.set_enabled("main/one".into(), false, 0).await?;
    p.access_log_control(false, 1).await?;
    assert!(records(&path)?.is_empty());
    io.shutdown().await?;
    assert_eq!(
        timeout(Duration::from_secs(3), io.read_u8())
            .await?
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    let found = wait_records(&p, &path, 1).await?;
    assert_eq!(found[0]["protocol"], "tcp");
    assert_eq!(found[0]["generation"], 0);
    assert_eq!(found[0]["body_bytes"], 14);
    assert_eq!(found[0]["outcome"], "complete");
    let mut rejected = TcpStream::connect(f.address()).await?;
    assert!(timeout(Duration::from_secs(3), rejected.read_u8())
        .await?
        .is_err());
    let found = wait_records(&p, &path, 2).await?;
    assert_eq!(found[1]["generation"], 1);
    assert_eq!(found[1]["outcome"], "error");
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn websocket_is_logged_at_tunnel_end_with_transport_lifetime() -> Result<()> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let address = l.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(io).await {
                    while let Some(Ok(message)) = ws.next().await {
                        if ws.send(message).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });
    let backend = Server { address, task };
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let path = enable(&mut f)?;
    let p = f.start().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/socket", f.address())).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("hello"))
        .await?;
    assert_eq!(
        timeout(Duration::from_secs(3), ws.next())
            .await?
            .unwrap()?
            .into_text()?,
        "hello"
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connections_open",
            &[("upstream", "main")]
        ),
        Some(1.)
    );
    p.set_enabled("main/one".into(), false, 0).await?;
    p.access_log_control(false, 1).await?;
    assert!(records(&path)?.is_empty());
    drop(ws);
    let found = wait_records(&p, &path, 1).await?;
    assert_eq!(found[0]["protocol"], "websocket");
    assert_eq!(found[0]["status"], 101);
    assert_eq!(found[0]["generation"], 0);
    assert!(found[0]["body_bytes"].as_u64().unwrap() >= 10);
    p.shutdown().await;
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connections_open",
            &[("upstream", "main")]
        ),
        Some(0.)
    );
    Ok(())
}
