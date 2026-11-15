use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_v2_h2_fanout_reuses_parallel_h1_connections_without_churn() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let opened = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(4));
    let counter = opened.clone();
    let task = tokio::spawn(async move {
        while let Ok((mut io, _)) = listener.accept().await {
            let counter = counter.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                let result: Result<()> = async {
                    let mut header = [0u8; 16];
                    io.read_exact(&mut header).await?;
                    if header[12..] == [0x20, 0, 0, 0] {
                        return Ok(());
                    }
                    anyhow::ensure!(header[12..] == [0x21, 0x11, 0, 12]);
                    let mut addresses = [0u8; 12];
                    io.read_exact(&mut addresses).await?;
                    anyhow::ensure!(addresses[..4] == [127, 0, 0, 1]);
                    counter.fetch_add(1, Ordering::Relaxed);
                    let service = service_fn(move |_: Request<Incoming>| {
                        let barrier = barrier.clone();
                        async move {
                            barrier.wait().await;
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                        }
                    });
                    hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(io), service)
                        .await?;
                    Ok(())
                }
                .await;
                let _ = result;
            });
        }
    });
    let backend = Server { address, task };
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let upstream = f.config.upstreams.get_mut("main").unwrap();
    upstream.send_proxy_v2 = true;
    upstream.health_check.proxy_v2_command = prost_config::ProxyV2Command::Local;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    for _ in 0..10 {
        let requests = (0..4).map(|_| {
            let mut client = client.clone();
            async move {
                let response = client
                    .send_request(
                        Request::builder()
                            .uri("http://mail.test/")
                            .body(Full::new(Bytes::new()))?,
                    )
                    .await?;
                anyhow::ensure!(response.status() == 200);
                anyhow::ensure!(response.into_body().collect().await?.to_bytes() == "ok");
                Ok::<_, anyhow::Error>(())
            }
        });
        timeout(
            Duration::from_secs(3),
            futures_util::future::try_join_all(requests),
        )
        .await??;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        opened.load(Ordering::Relaxed),
        4,
        "fanout pool must reuse all four transports"
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_health_headers_are_empty_and_traffic_keeps_real_proxy_addresses() -> Result<()> {
    for http_check in [false, true] {
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let address = socket.local_addr()?;
        let probes = Arc::new(AtomicUsize::new(0));
        let clients = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(AtomicUsize::new(0));
        let (p, c, e) = (probes.clone(), clients.clone(), errors.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut io, _)) = socket.accept().await {
                let (p, c, e) = (p.clone(), c.clone(), e.clone());
                tokio::spawn(async move {
                    let result: Result<()> = async {
                        let mut frame = [0u8; 16];
                        io.read_exact(&mut frame).await?;
                        anyhow::ensure!(&frame[..12] == b"\r\n\r\n\0\r\nQUIT\n");
                        let local = frame[12] == 0x20;
                        if local {
                            anyhow::ensure!(
                                frame[13..] == [0, 0, 0],
                                "LOCAL must have no addresses/TLVs"
                            );
                            if !http_check {
                                p.fetch_add(1, Ordering::Relaxed);
                                return Ok(());
                            }
                        } else {
                            anyhow::ensure!(frame[12..] == [0x21, 0x11, 0, 12]);
                            let mut payload = [0u8; 12];
                            io.read_exact(&mut payload).await?;
                            anyhow::ensure!(payload[..4] == [127, 0, 0, 1]);
                            c.fetch_add(1, Ordering::Relaxed);
                        }
                        let service = service_fn(move |request: Request<Incoming>| {
                            let p = p.clone();
                            async move {
                                if local {
                                    assert_eq!(request.uri().path(), "/health");
                                    p.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    assert_eq!(request.uri().path(), "/");
                                }
                                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(
                                    b"ok",
                                ))))
                            }
                        });
                        hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(io), service)
                            .await?;
                        Ok(())
                    }
                    .await;
                    if result.is_err() {
                        e.fetch_add(1, Ordering::Relaxed);
                    }
                });
            }
        });
        let backend = Server { address, task };
        let mut f = Fixture::new(Protocol::Http, backend.address);
        let upstream = f.config.upstreams.get_mut("main").unwrap();
        upstream.send_proxy_v2 = true;
        upstream.health_check.proxy_v2_command = prost_config::ProxyV2Command::Local;
        if http_check {
            upstream.health_check.protocol = prost_config::HealthProtocol::Http;
            upstream.health_check.path = Some("/health".into());
        }
        std::fs::write(&f.path, f.config.encode()?)?;
        let proxy = f.start().await;
        let mut io = TcpStream::connect(f.address()).await?;
        io.write_all(b"GET / HTTP/1.1\r\nHost: mail.test\r\nConnection: close\r\n\r\n")
            .await?;
        let mut response = Vec::new();
        timeout(Duration::from_secs(3), io.read_to_end(&mut response)).await??;
        assert!(response.starts_with(b"HTTP/1.1 200"), "{response:?}");
        assert!(response.ends_with(b"ok"));
        timeout(Duration::from_secs(3), async {
            while probes.load(Ordering::Relaxed) < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        assert_eq!(clients.load(Ordering::Relaxed), 1);
        assert_eq!(errors.load(Ordering::Relaxed), 0);
        let mut invalid = proxy.snapshot().config.clone();
        invalid
            .upstreams
            .get_mut("main")
            .unwrap()
            .health_check
            .send_proxy_v2 = Some(false);
        assert!(invalid.validate().is_err());
        proxy.shutdown().await;
    }
    Ok(())
}
