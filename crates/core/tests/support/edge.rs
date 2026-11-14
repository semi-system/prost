use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_acl_h1_h2_redirect_order_and_hot_update() -> Result<()> {
    let backend = http_server("ready").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].plugins = vec![
        builtin("host_acl", serde_json::json!({"hosts":["public.test"]}))?,
        builtin(
            "rewrite",
            serde_json::json!({"rules":[{"action":"redirect","prefix":"/old","to":"/new","status":308}]}),
        )?,
    ];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut h1, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?))
            .await?;
    let driver = tokio::spawn(conn);
    for (host, path, status) in [
        ("PUBLIC.TEST:8080", "/", 200),
        ("evil.test", "/", 421),
        ("evil.test", "/old", 421),
        ("public.test", "/old?x=1", 308),
    ] {
        let response = h1
            .send_request(
                Request::builder()
                    .uri(path)
                    .header("host", host)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), status);
        if status == 308 {
            assert_eq!(response.headers()["location"], "/new?x=1");
        }
        response.into_body().collect().await?;
    }
    let (mut h2, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let d2 = tokio::spawn(conn);
    for (authority, status) in [("public.test", 200), ("evil.test", 421)] {
        let response = h2
            .send_request(
                Request::builder()
                    .uri(format!("http://{authority}/"))
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), status);
        response.into_body().collect().await?;
    }
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[0] =
        builtin("host_acl", serde_json::json!({"hosts":["next.test"]}))?;
    p.apply(config.encode()?, 0).await?;
    for (host, status) in [("public.test", 421), ("next.test", 200)] {
        let response = h1
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", host)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), status);
        response.into_body().collect().await?;
    }
    assert_eq!(
        metric(
            &p,
            "prost_upstream_selections_total",
            &[("upstream", "main"), ("endpoint", "one")]
        ),
        Some(3.)
    );
    drop(h1);
    drop(h2);
    driver.abort();
    d2.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_keepalive_is_longer_than_header_budget_and_closes_without_408() -> Result<()> {
    let backend = http_server("ready").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].header_read_timeout_ms = 200;
    f.config.listeners[0].keepalive_idle_timeout_ms = Some(700);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?))
            .await?;
    let driver = tokio::spawn(conn);
    let request = || {
        Request::builder()
            .uri("/")
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    assert_eq!(
        client
            .send_request(request())
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "ready"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        client
            .send_request(request())
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "ready"
    );
    timeout(Duration::from_secs(2), driver).await???;
    assert_eq!(
        p.metrics.errors.get(),
        0,
        "idle expiry should be a quiet close"
    );
    drop(client);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_budget_does_not_cancel_h1_response_body_or_h2_connections() -> Result<()> {
    let backend = http_server("ready").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].header_read_timeout_ms = 300;
    f.config.listeners[0].keepalive_idle_timeout_ms = Some(80);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let request = |path: &str| {
        Request::builder()
            .uri(path)
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let (mut h1, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?))
            .await?;
    let driver = tokio::spawn(conn);
    assert_eq!(
        h1.send_request(request("/slow"))
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "late"
    );
    drop(h1);
    driver.abort();
    let (mut h2, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(conn);
    h2.send_request(request("http://localhost/"))
        .await?
        .into_body()
        .collect()
        .await?;
    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(
        h2.send_request(request("http://localhost/"))
            .await?
            .status(),
        200
    );
    drop(h2);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn buffered_pipelined_partial_header_keeps_the_header_deadline() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let backend = http_server("ready").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].header_read_timeout_ms = 200;
    f.config.listeners[0].keepalive_idle_timeout_ms = Some(2000);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut io = BufReader::new(TcpStream::connect(f.address()).await?);
    io.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\nGET /partial HTTP/1.1\r\nHost: local")
        .await?;
    let mut head = Vec::new();
    loop {
        io.read_until(b'\n', &mut head).await?;
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    assert!(head.starts_with(b"HTTP/1.1 200"));
    let mut body = [0u8; 5];
    io.read_exact(&mut body).await?;
    assert_eq!(&body, b"ready");
    let mut rest = Vec::new();
    timeout(Duration::from_millis(1200), io.read_to_end(&mut rest)).await??;
    assert_eq!(p.metrics.requests.get(), 1);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_retry_recovers_before_payload_without_ejecting_endpoint() -> Result<()> {
    let initial = TcpListener::bind("127.0.0.1:0").await?;
    let address = initial.local_addr()?;
    let mut f = Fixture::new(Protocol::Tcp, address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.health_interval_ms = 60000;
    u.tcp_retry = Some(prost_config::TcpRetry {
        max_attempts: 6,
        timeout_ms: 1500,
        backoff_ms: 50,
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    timeout(Duration::from_secs(2), async {
        while metric(
            &p,
            "prost_upstream_health_probes_total",
            &[
                ("upstream", "main"),
                ("endpoint", "one"),
                ("result", "success"),
            ],
        )
        .unwrap_or(0.)
            < 1.
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(initial);
    let mut io = TcpStream::connect(f.address()).await?;
    io.write_all(b"payload once").await?;
    timeout(Duration::from_secs(2), async {
        while metric(
            &p,
            "prost_tcp_connect_retries_total",
            &[("upstream", "main"), ("endpoint", "one")],
        )
        .unwrap_or(0.)
            < 1.
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let listener = TcpListener::bind(address).await?;
    let backend = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut body = Vec::new();
        connection.read_to_end(&mut body).await.unwrap();
        assert_eq!(body, b"payload once");
        connection.write_all(&body).await.unwrap();
    });
    io.shutdown().await?;
    let mut body = Vec::new();
    timeout(Duration::from_secs(2), io.read_to_end(&mut body)).await??;
    assert_eq!(body, b"payload once");
    backend.await?;
    assert_eq!(p.status()["upstreams"]["main"][0]["healthy"], true);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_retry_exhaustion_total_budget_and_tls_failure_are_bounded() -> Result<()> {
    let address = free_address();
    let f = Fixture::new(Protocol::Tcp, address);
    let mut config = f.config.upstreams["main"].clone();
    config.tcp_retry = Some(prost_config::TcpRetry {
        max_attempts: 3,
        timeout_ms: 1000,
        backoff_ms: 1,
    });
    let counter = AtomicU64::new(0);
    assert!(prost_transport::connect_tcp(
        &address.to_string(),
        &config,
        prost_transport::client_tls(None)?,
        None,
        &counter
    )
    .await
    .is_err());
    assert_eq!(counter.load(Ordering::Relaxed), 2);
    config.tcp_retry = Some(prost_config::TcpRetry {
        max_attempts: 8,
        timeout_ms: 50,
        backoff_ms: 100,
    });
    counter.store(0, Ordering::Relaxed);
    let error = prost_transport::connect_tcp(
        &address.to_string(),
        &config,
        prost_transport::client_tls(None)?,
        None,
        &counter,
    )
    .await
    .err()
    .context("expected total retry timeout")?;
    assert!(error.to_string().contains("total timeout"));
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    let listener = TcpListener::bind(address).await?;
    let accepted = Arc::new(AtomicUsize::new(0));
    let calls = accepted.clone();
    let backend = tokio::spawn(async move {
        while let Ok((mut io, _)) = listener.accept().await {
            calls.fetch_add(1, Ordering::Relaxed);
            let mut preface = [0u8; 28];
            io.read_exact(&mut preface).await.unwrap();
            assert_eq!(&preface[..12], b"\r\n\r\n\0\r\nQUIT\n");
            io.write_all(b"not TLS").await.unwrap();
        }
    });
    config.send_proxy_v2 = true;
    config.tls = Some(prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: None,
        identity: None,
        policy: None,
    });
    config.tcp_retry = Some(Default::default());
    config.connect_timeout_ms = 200;
    counter.store(0, Ordering::Relaxed);
    assert!(prost_transport::connect_tcp(
        &address.to_string(),
        &config,
        prost_transport::client_tls(config.tls.as_ref())?,
        None,
        &counter
    )
    .await
    .is_err());
    assert_eq!(counter.load(Ordering::Relaxed), 0);
    assert_eq!(accepted.load(Ordering::Relaxed), 1);
    backend.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_window_is_shared_across_workers_and_compatible_apply() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.runtime.io_workers = 2;
    f.config.listeners[0].plugins.push(builtin(
        "connection_window",
        serde_json::json!({"max_connections":2,"window_ms":10000,"max_keys":1}),
    )?);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for _ in 0..2 {
        let mut io = TcpStream::connect(f.address()).await?;
        roundtrip(&mut io, b"allowed").await?;
    }
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
    p.apply(next.encode()?, 0).await?;
    let mut io = TcpStream::connect(f.address()).await?;
    assert!(timeout(Duration::from_secs(1), io.read_u8())
        .await?
        .is_err());
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_keepalive_reuses_beyond_header_budget_and_sends_clean_tls_close() -> Result<()> {
    let backend = http_server("secure").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let tls = certificate(f.dir.path())?;
    f.config.listeners[0].tls = Some(tls.clone());
    f.config.listeners[0].header_read_timeout_ms = 200;
    f.config.listeners[0].keepalive_idle_timeout_ms = Some(700);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let tls_client = prost_transport::client_tls(Some(&prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate),
        identity: None,
        policy: None,
    }))?;
    let io = tokio_rustls::TlsConnector::from(tls_client)
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(f.address()).await?,
        )
        .await?;
    let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    let request = || {
        Request::builder()
            .uri("/")
            .header("host", "localhost")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    assert_eq!(
        client
            .send_request(request())
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "secure"
    );
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(
        client
            .send_request(request())
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "secure"
    );
    timeout(Duration::from_secs(2), driver).await???;
    assert_eq!(p.metrics.errors.get(), 0);
    drop(client);
    p.shutdown().await;
    Ok(())
}
