use super::*;

const LABEL: &[(&str, &str)] = &[("upstream", "main")];
fn request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://localhost{path}"))
        .body(Full::new(Bytes::new()))
        .unwrap()
}
async fn wait_open(p: &RunningProxy, value: f64) -> Result<()> {
    timeout(Duration::from_secs(3), async {
        while metric(p, "prost_http_upstream_connections_open", LABEL) != Some(value) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h1_connections_reuse_pool_replacement_and_draining_transports() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.observe_connection_reuse = true;
    u.pool_idle_timeout_ms = 10000;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    for _ in 0..5 {
        client
            .send_request(request("/"))
            .await?
            .into_body()
            .collect()
            .await?;
    }
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_opened_total", LABEL),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connection_responses_total",
            &[("upstream", "main"), ("usage", "first")]
        ),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connection_responses_total",
            &[("upstream", "main"), ("usage", "reused")]
        ),
        Some(4.)
    );
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
    p.apply(next.encode()?, 0).await?;
    client
        .send_request(request("/"))
        .await?
        .into_body()
        .collect()
        .await?;
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_opened_total", LABEL),
        Some(1.)
    );
    let mut held = client.send_request(request("/hold")).await?;
    held.body_mut().frame().await.unwrap()?;
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().pool_idle_timeout_ms = 150;
    p.apply(next.encode()?, 1).await?;
    client
        .send_request(request("/"))
        .await?
        .into_body()
        .collect()
        .await?;
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_opened_total", LABEL),
        Some(2.)
    );
    wait_open(&p, 2.).await?;
    // A removed pool still owns a live stream; its transport remains counted.
    drop(held);
    wait_open(&p, 0.).await?;
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_closed_total", LABEL),
        Some(2.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connect_total",
            &[("upstream", "main"), ("result", "success")]
        ),
        Some(2.)
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_multiplexing_counts_responses_on_one_transport() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.observe_connection_reuse = true;
    u.http2 = true;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    let tasks = (0..20).map(|_| {
        let mut c = client.clone();
        async move {
            c.send_request(request("/grpc"))
                .await?
                .into_body()
                .collect()
                .await
                .map_err(anyhow::Error::from)
        }
    });
    for result in futures_util::future::join_all(tasks).await {
        assert_eq!(result?.trailers().unwrap()["grpc-status"], "0");
    }
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_opened_total", LABEL),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connection_responses_total",
            &[("upstream", "main"), ("usage", "first")]
        ),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connection_responses_total",
            &[("upstream", "main"), ("usage", "reused")]
        ),
        Some(19.)
    );
    let mut next = p.snapshot().config.clone();
    next.upstreams
        .get_mut("main")
        .unwrap()
        .observe_connection_reuse = false;
    p.apply(next.encode()?, 0).await?;
    let mut client = client;
    client
        .send_request(request("/"))
        .await?
        .into_body()
        .collect()
        .await?;
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connection_responses_total",
            &[("upstream", "main"), ("usage", "reused")]
        ),
        Some(19.)
    );
    assert_eq!(
        metric(&p, "prost_http_upstream_reuse_tracking", LABEL),
        Some(0.)
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn refused_connect_counts_error_without_opening_a_transport() -> Result<()> {
    let mut f = Fixture::new(Protocol::Http, free_address());
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.health_check.fall = 100;
    u.health_interval_ms = 60000;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let response = limits::raw_response(
        f.address(),
        b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await?;
    assert!(response.starts_with("HTTP/1.1 503"));
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connect_total",
            &[("upstream", "main"), ("result", "error")]
        ),
        Some(1.)
    );
    assert_eq!(
        metric(&p, "prost_http_upstream_connecting", LABEL),
        Some(0.)
    );
    assert_eq!(
        metric(&p, "prost_http_upstream_connections_opened_total", LABEL),
        Some(0.)
    );
    p.shutdown().await;
    Ok(())
}
