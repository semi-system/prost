use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_h2_window_holds_last_frame_budget_and_logs_abort() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].http2_settings.stream_write_timeout_ms = 150;
    f.config.upstreams.get_mut("main").unwrap().endpoints[0].max_active = 1;
    let log = f.dir.path().join("window.jsonl");
    f.config.runtime.access_log = Some(prost_config::AccessLog {
        path: log.clone(),
        ..Default::default()
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(1)
        .handshake(TokioIo::new(TcpStream::connect(f.address()).await?))
        .await?;
    let driver = tokio::spawn(conn);
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
    p.apply(next.encode()?, 0).await?;
    // Holding the body prevents Hyper's client from releasing receive capacity.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        timeout(Duration::from_secs(2), response.into_body().collect())
            .await?
            .is_err()
    );
    assert_eq!(
        metric(&p, "prost_h2_stream_write_timeouts_total", &[]),
        Some(1.)
    );
    let response = client
        .send_request(
            Request::builder()
                .method("HEAD")
                .uri("http://localhost/")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 200);
    response.into_body().collect().await?;
    p.access_log_control(false, 1).await?;
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(log)?
        .lines()
        .map(serde_json::from_str)
        .collect::<std::result::Result<_, _>>()?;
    assert!(rows
        .iter()
        .any(|r| r["generation"] == 0 && r["outcome"] == "aborted"));
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn downstream_mtls_requires_identity_and_recovers_from_artifacts() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    let mut tls = certificate(f.dir.path())?;
    tls.client_ca = Some(tls.certificate.clone());
    let client_config = prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate.clone()),
        identity: Some(prost_config::ClientIdentity {
            certificate: tls.certificate.clone(),
            private_key: tls.private_key.clone(),
        }),
    };
    let good = prost_transport::client_tls(Some(&client_config))?;
    let mut no_identity = client_config.clone();
    no_identity.identity = None;
    let bad = prost_transport::client_tls(Some(&no_identity))?;
    f.config.listeners[0].tls = Some(tls.clone());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let name = rustls::pki_types::ServerName::try_from("localhost")?;
    let rejected = async {
        let mut io = tokio_rustls::TlsConnector::from(bad)
            .connect(name.clone(), TcpStream::connect(f.address()).await?)
            .await?;
        roundtrip(&mut io, b"denied").await
    }
    .await;
    assert!(rejected.is_err());
    let mut io = tokio_rustls::TlsConnector::from(good.clone())
        .connect(name.clone(), TcpStream::connect(f.address()).await?)
        .await?;
    roundtrip(&mut io, b"allowed").await?;
    drop(io);
    p.shutdown().await;
    drop(p);
    std::fs::remove_file(tls.certificate)?;
    std::fs::remove_file(tls.private_key)?;
    let p = f.start().await;
    let mut io = tokio_rustls::TlsConnector::from(good)
        .connect(name, TcpStream::connect(f.address()).await?)
        .await?;
    roundtrip(&mut io, b"restart").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_busy_pool_metrics_follow_held_and_cancelled_streams() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config
        .upstreams
        .get_mut("main")
        .unwrap()
        .observe_pool_state = true;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(conn);
    let request = |path: &str| {
        Request::builder()
            .uri(format!("http://localhost{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let mut held = client.send_request(request("/hold")).await?;
    held.body_mut().frame().await.unwrap()?;
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connections_busy",
            &[("upstream", "main")]
        ),
        Some(1.)
    );
    client
        .send_request(request("/"))
        .await?
        .into_body()
        .collect()
        .await?;
    timeout(Duration::from_secs(2), async {
        while metric(
            &p,
            "prost_http_upstream_connections_idle",
            &[("upstream", "main")],
        ) != Some(1.)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(held);
    drop(client);
    driver.abort();
    timeout(Duration::from_secs(2), async {
        while metric(
            &p,
            "prost_http_upstream_connections_busy",
            &[("upstream", "main")],
        ) != Some(0.)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn plugin_metrics_and_compact_log_filters() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let log = f.dir.path().join("compact.jsonl");
    f.config.runtime.observe_plugins = true;
    f.config.runtime.access_log = Some(prost_config::AccessLog {
        path: log.clone(),
        format: prost_config::LogFormat::Compact,
        statuses: vec![503],
        include_paths: vec!["/status/".into()],
        exclude_paths: vec!["/status/404".into()],
        ..Default::default()
    });
    f.config.listeners[0].plugins.push(builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-native":"yes"}}}),
    )?);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) =
        crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
    let driver = tokio::spawn(conn);
    for path in ["/", "/status/200", "/status/404", "/status/503"] {
        let response = client
            .send_request(
                Request::builder()
                    .uri(path)
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        response.into_body().collect().await?;
    }
    p.access_log_control(false, 0).await?;
    let raw = std::fs::read_to_string(log)?;
    let lines: Vec<_> = raw.lines().collect();
    assert_eq!(lines.len(), 1);
    let record: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(record["s"], 503);
    assert_eq!(record["u"], "/status/503");
    assert_eq!(
        metric(&p, "prost_access_log_records_filtered_total", &[]),
        Some(3.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_plugin_calls_total",
            &[
                ("plugin", "headers"),
                ("hook", "response_headers"),
                ("result", "success")
            ]
        ),
        Some(4.)
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn h2_continuation_deadline_covers_headers_after_first_request() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].http2_settings.header_read_timeout_ms = 80;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut io = TcpStream::connect(f.address()).await?;
    io.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await?;
    io.write_all(&[0, 0, 0, 4, 0, 0, 0, 0, 0]).await?;
    let mut block = vec![0x82, 0x86, 0x84, 0x01, 9];
    block.extend_from_slice(b"localhost");
    let mut frame = vec![0, 0, block.len() as u8, 1, 5, 0, 0, 0, 1];
    frame.extend(block);
    io.write_all(&frame).await?;
    timeout(Duration::from_secs(2), async {
        while p.metrics.requests.get() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    io.write_all(&[0, 0, 1, 1, 1, 0, 0, 0, 3, 0x82]).await?;
    timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        let _ = io.read_to_end(&mut bytes).await;
    })
    .await?;
    assert_eq!(p.metrics.requests.get(), 1);
    p.shutdown().await;
    Ok(())
}
