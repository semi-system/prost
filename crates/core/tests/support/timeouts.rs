use super::*;

#[test]
fn timeout_config_defaults_roundtrip_and_invalid_values() -> Result<()> {
    let f = Fixture::new(Protocol::Http, "127.0.0.1:9000".parse()?);
    let mut config = f.config;
    assert_eq!(config.listeners[0].header_read_timeout_ms, 10000);
    assert_eq!(config.listeners[0].max_request_body_bytes, None);
    assert_eq!(config.upstreams["main"].response_header_timeout_ms, None);
    config.listeners[0].request_body_timeout_ms = Some(86400001);
    assert!(config.validate().is_err());
    config.listeners[0].request_body_timeout_ms = Some(0);
    config.upstreams.get_mut("main").unwrap().write_timeout_ms = Some(86400001);
    assert!(config.validate().is_err());
    config.upstreams.get_mut("main").unwrap().write_timeout_ms = Some(1000);
    let decoded = Config::parse(&config.encode()?)?;
    assert_eq!(decoded.listeners[0].request_body_timeout_ms, Some(0));
    assert_eq!(decoded.upstreams["main"].write_timeout_ms, Some(1000));
    config.listeners[0].protocol = Protocol::Tcp;
    assert!(config.validate().is_err());
    Ok(())
}
async fn stalled_backend() -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut peers = Vec::new();
        loop {
            if let Ok((io, _)) = listener.accept().await {
                peers.push(io);
            }
        }
    });
    Server { address, task }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_deadline_includes_stalled_tls_with_header_deadline_disabled() -> Result<()> {
    let backend = stalled_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.connect_timeout_ms = 100;
    u.response_header_timeout_ms = Some(0);
    u.tls = Some(prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        identity: None,
        ca_certificate: None,
    });
    u.health_interval_ms = 60000;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let response = timeout(
        Duration::from_secs(1),
        limits::raw_response(
            f.address(),
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        ),
    )
    .await??;
    assert!(response.starts_with("HTTP/1.1 503"), "{response}");
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connect_total",
            &[("upstream", "main"), ("result", "timeout")]
        ),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_connecting",
            &[("upstream", "main")]
        ),
        Some(0.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_http_upstream_tls_connect_attempts_total",
            &[("upstream", "main")]
        ),
        Some(1.)
    );
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_write_deadline_stops_upload_to_backend_that_never_reads() -> Result<()> {
    let backend = stalled_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.write_timeout_ms = Some(100);
    u.response_header_timeout_ms = Some(0);
    u.health_interval_ms = 60000;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let io = TcpStream::connect(f.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    static CHUNK: [u8; 16384] = [b'x'; 16384];
    let frames = futures_util::stream::repeat_with(|| {
        Ok::<_, Infallible>(Frame::data(Bytes::from_static(&CHUNK)))
    })
    .take(8192);
    let request = Request::builder()
        .method("POST")
        .uri("http://localhost/")
        .body(StreamBody::new(frames))
        .unwrap();
    let response = timeout(Duration::from_secs(3), client.send_request(request)).await??;
    assert_eq!(response.status(), 503);
    response.into_body().collect().await?;
    assert_eq!(
        metric(
            &p,
            "prost_upstream_forward_errors_total",
            &[("upstream", "main"), ("endpoint", "one")]
        ),
        Some(1.)
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_socket_write_deadline_releases_slow_reader_connection() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let backend = Server {
        address,
        task: tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                tokio::spawn(async move {
                    static CHUNK: [u8; 16384] = [b'x'; 16384];
                    let service = service_fn(|_: Request<Incoming>| async {
                        let frames = futures_util::stream::repeat_with(|| {
                            Ok::<_, Infallible>(Frame::data(Bytes::from_static(&CHUNK)))
                        });
                        Ok::<_, Infallible>(Response::new(StreamBody::new(frames)))
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                });
            }
        }),
    };
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].client_write_timeout_ms = Some(100);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut io = TcpStream::connect(f.address()).await?;
    socket2::SockRef::from(&io).set_recv_buffer_size(1024)?;
    io.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await?;
    timeout(Duration::from_secs(2), async {
        while p.metrics.connections.get() == 0 {
            tokio::task::yield_now().await;
        }
        while p.metrics.connections.get() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(metric(&p, "prost_requests_active", &[]), Some(0.));
    drop(io);
    p.shutdown().await;
    Ok(())
}
