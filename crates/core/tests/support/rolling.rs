use super::*;
use tokio_util::sync::CancellationToken;

async fn tagged(label: u8) -> Result<Server> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((mut io, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _ = io.write_all(&[label]).await;
                let (mut r, mut w) = io.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    Ok(Server { address, task })
}
async fn streaming(label: &'static str, release: CancellationToken) -> Result<Server> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let release = release.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let release = release.clone();
                    async move {
                        let grpc = request.uri().path() == "/grpc";
                        let body = if request.uri().path() == "/hold" || grpc {
                            let first = futures_util::stream::once(async move {
                                Ok::<_, Infallible>(Frame::data(if grpc {
                                    Bytes::from_static(b"\0\0\0\0\x01a")
                                } else {
                                    Bytes::from_static(label.as_bytes())
                                }))
                            });
                            let last = futures_util::stream::once(async move {
                                release.cancelled().await;
                                Ok::<_, Infallible>(Frame::data(if grpc {
                                    Bytes::from_static(b"\0\0\0\0\x03end")
                                } else {
                                    Bytes::from_static(b"end")
                                }))
                            });
                            let trailers = futures_util::stream::iter(if grpc {
                                let mut h = http::HeaderMap::new();
                                h.insert("grpc-status", "0".parse().unwrap());
                                vec![Ok::<_, Infallible>(Frame::trailers(h))]
                            } else {
                                vec![]
                            });
                            StreamBody::new(first.chain(last).chain(trailers)).boxed_unsync()
                        } else {
                            Full::new(Bytes::from_static(label.as_bytes())).boxed_unsync()
                        };
                        let mut response = Response::new(body);
                        if grpc {
                            response
                                .headers_mut()
                                .insert("content-type", "application/grpc".parse().unwrap());
                        }
                        Ok::<_, Infallible>(response)
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
fn request(path: &str) -> Request<Full<Bytes>> {
    Request::builder()
        .uri(format!("http://localhost{path}"))
        .header("host", "localhost")
        .body(Full::new(Bytes::new()))
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_workers_drain_h1_h2_keep_tcp_and_restore_on_bind_failure() -> Result<()> {
    let release = CancellationToken::new();
    let a = streaming("a", release.clone()).await?;
    let b = http_server("b").await;
    let tcp_a = tagged(b'A').await?;
    let tcp_b = tagged(b'B').await?;
    let mut f = Fixture::new(Protocol::Http, a.address);
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    f.config.runtime.io_workers = 2;
    f.config.runtime.drain_seconds = 5;
    f.config.upstreams.get_mut("main").unwrap().http2 = true;
    f.config.listeners[0].plugins.push(builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-generation":"0"}}}),
    )?);
    let mut tcp = f.config.upstreams["main"].clone();
    tcp.endpoints[0].address = tcp_a.address.to_string();
    f.config.upstreams.insert("tcp".into(), tcp);
    let mut listener = f.config.listeners[0].clone();
    listener.id = "tcp".into();
    listener.protocol = Protocol::Tcp;
    listener.address = free_address();
    listener.upstream = Some("tcp".into());
    listener.plugins.clear();
    let tcp_address = listener.address;
    f.config.listeners.push(listener);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let old = Arc::downgrade(&p.snapshot());
    let (mut h1, c1) =
        crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
    let d1 = tokio::spawn(c1);
    let (mut h2, c2) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let d2 = tokio::spawn(c2);
    let mut one = h1.send_request(request("/hold")).await?;
    let mut two = h2.send_request(request("/grpc")).await?;
    assert_eq!(one.headers()["x-generation"], "0");
    assert_eq!(
        one.body_mut().frame().await.unwrap()?.into_data().unwrap(),
        "a"
    );
    assert_eq!(
        two.body_mut().frame().await.unwrap()?.into_data().unwrap(),
        Bytes::from_static(b"\0\0\0\0\x01a")
    );
    let mut old_tcp = TcpStream::connect(tcp_address).await?;
    assert_eq!(old_tcp.read_u8().await?, b'A');
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].address = b.address.to_string();
    next.upstreams.get_mut("tcp").unwrap().endpoints[0].address = tcp_b.address.to_string();
    next.listeners[0].plugins[0] = builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-generation":"1"}}}),
    )?;
    p.apply(next.encode()?, 0).await?;
    assert_eq!(p.status()["worker_generation"], 1);
    assert_eq!(p.status()["retiring_worker_generation"], 0);
    let (mut fresh, connection) =
        crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
    let driver = tokio::spawn(connection);
    let response = fresh.send_request(request("/")).await?;
    assert_eq!(response.headers()["x-generation"], "1");
    assert_eq!(response.into_body().collect().await?.to_bytes(), "b");
    let mut new_tcp = TcpStream::connect(tcp_address).await?;
    assert_eq!(new_tcp.read_u8().await?, b'B');
    roundtrip(&mut old_tcp, b"old TCP continues").await?;
    release.cancel();
    assert_eq!(one.into_body().collect().await?.to_bytes(), "end");
    let grpc = two.into_body().collect().await?;
    assert_eq!(grpc.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(grpc.to_bytes(), Bytes::from_static(b"\0\0\0\0\x03end"));
    drop(h1);
    drop(h2);
    d1.abort();
    d2.abort();
    drop(old_tcp);
    assert_eq!(p.wait_for_workers(1, 5000).await?["drained"], true);
    assert_eq!(
        metric(&p, "prost_worker_generations_retired_total", &[]),
        Some(1.)
    );
    assert_eq!(
        metric(&p, "prost_worker_generations_forced_total", &[]),
        Some(0.)
    );
    assert!(old.upgrade().is_none());
    let occupied = TcpListener::bind("127.0.0.1:0").await?;
    let mut bad = p.snapshot().config.clone();
    let mut extra = bad.listeners[0].clone();
    extra.id = "bad".into();
    extra.address = occupied.local_addr()?;
    bad.listeners.push(extra);
    let durable = std::fs::read(&f.path)?;
    assert!(p.apply(bad.encode()?, 1).await.is_err());
    assert_eq!(p.status()["worker_generation"], 1);
    assert_eq!(std::fs::read(&f.path)?, durable);
    assert_eq!(fresh.send_request(request("/")).await?.status(), 200);
    drop(new_tcp);
    drop(fresh);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_queue_limits_generations_and_forces_old_sessions_at_deadline() -> Result<()> {
    let a = tagged(b'A').await?;
    let mut f = Fixture::new(Protocol::Tcp, a.address);
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    f.config.runtime.drain_seconds = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut old = TcpStream::connect(f.address()).await?;
    assert_eq!(old.read_u8().await?, b'A');
    let text = p.snapshot().config.encode()?;
    let (one, two) = tokio::join!(p.apply(text.clone(), 0), p.apply(text, 0));
    assert!(one.is_ok() ^ two.is_ok());
    assert!(
        format!("{:#}", one.err().or_else(|| two.err()).unwrap()).contains("generation_conflict")
    );
    roundtrip(&mut old, b"draining").await?;
    let pending = tokio::spawn({
        let p = p.clone();
        let text = p.snapshot().config.encode()?;
        async move { p.apply(text, 1).await }
    });
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(5), old.read_to_end(&mut bytes)).await??;
    assert!(bytes.is_empty());
    timeout(Duration::from_secs(5), pending).await???;
    p.wait_for_workers(2, 5000).await?;
    assert_eq!(
        metric(&p, "prost_worker_generations_retired_total", &[]),
        Some(2.)
    );
    assert_eq!(
        metric(&p, "prost_worker_generations_forced_total", &[]),
        Some(1.)
    );
    assert_eq!(
        metric(&p, "prost_worker_connections_forced_total", &[]),
        Some(1.)
    );
    assert_eq!(p.status()["worker_generation"], 2);
    assert_eq!(p.snapshot().config.generation, 2);
    let mut fresh = TcpStream::connect(f.address()).await?;
    assert_eq!(fresh.read_u8().await?, b'A');
    roundtrip(&mut fresh, b"new generation").await?;
    drop(fresh);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_wait_barrier_releases_coordinator_and_timeout_preserves_commit() -> Result<()> {
    let backend = tagged(b'A').await?;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    f.config.runtime.drain_seconds = 30;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut old = TcpStream::connect(f.address()).await?;
    assert_eq!(old.read_u8().await?, b'A');
    p.apply(p.snapshot().config.encode()?, 0).await?;
    let disk = std::fs::read(&f.path)?;
    let error = p.wait_for_workers(1, 10).await.unwrap_err();
    assert!(format!("{error:#}").contains("worker_drain_timeout"));
    assert_eq!(p.snapshot().config.generation, 1);
    assert_eq!(std::fs::read(&f.path)?, disk);
    assert_eq!(
        p.status()["worker_generations"]["retiring"]["connections"],
        1
    );
    assert!(
        p.status()["worker_generations"]["retiring"]["drain_remaining_ms"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(metric(&p, "prost_worker_generations_live", &[]), Some(2.));
    assert_eq!(
        metric(&p, "prost_worker_retiring_connections", &[]),
        Some(1.)
    );
    for budget in [0, 300001] {
        assert!(p.wait_for_workers(1, budget).await.is_err());
    }
    assert!(p
        .wait_for_workers(2, 100)
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_not_committed"));
    assert_eq!(p.wait_for_workers(0, 100).await?["drained"], true);
    let mut barrier = Box::pin(p.wait_for_workers(1, 3000));
    assert!(timeout(Duration::from_millis(10), &mut barrier)
        .await
        .is_err());
    timeout(
        Duration::from_secs(1),
        p.validate(p.snapshot().config.encode()?),
    )
    .await??;
    let mut fresh = TcpStream::connect(f.address()).await?;
    assert_eq!(fresh.read_u8().await?, b'A');
    roundtrip(&mut fresh, b"accepts during wait").await?;
    roundtrip(&mut old, b"old stream during wait").await?;
    drop(old);
    assert_eq!(barrier.await?["drained"], true);
    assert_eq!(metric(&p, "prost_worker_generations_live", &[]), Some(1.));
    assert_eq!(
        metric(&p, "prost_worker_retiring_connections", &[]),
        Some(0.)
    );
    assert_eq!(
        metric(&p, "prost_worker_generations_retired_total", &[]),
        Some(1.)
    );
    assert_eq!(
        metric(&p, "prost_worker_generations_forced_total", &[]),
        Some(0.)
    );
    drop(fresh);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rolling_accept_before_first_headers_preserves_first_h1_h2_request() -> Result<()> {
    let a = http_server("a").await;
    let b = http_server("b").await;
    for header_timeout in [0, 10000] {
        let mut f = Fixture::new(Protocol::Http, a.address);
        f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
        f.config.runtime.drain_seconds = 5;
        f.config.listeners[0].header_read_timeout_ms = header_timeout;
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let first_h1 = TcpStream::connect(f.address()).await?;
        let first_h2 = TcpStream::connect(f.address()).await?;
        timeout(Duration::from_secs(2), async {
            while p.metrics.connections.get() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let mut next = p.snapshot().config.clone();
        next.upstreams.get_mut("main").unwrap().endpoints[0].address = b.address.to_string();
        p.apply(next.encode()?, 0).await?;
        let (mut h1, c1) = crate::http1::handshake(TokioIo::new(first_h1)).await?;
        let d1 = tokio::spawn(c1);
        let response = h1.send_request(request("/")).await?;
        assert_eq!(response.into_body().collect().await?.to_bytes(), "a");
        // Exercise the accepted stream and graceful GOAWAY directly. Hyper's
        // client dispatcher polls readiness again after opening a stream; a
        // concurrent GOAWAY can close that dispatch slot despite the server
        // having accepted the stream. The wire-level client retains its
        // response future independently of readiness for subsequent requests.
        let (mut h2, c2) = h2::client::handshake(first_h2)
            .await
            .context("H2 handshake on socket accepted before rolling")?;
        let d2 = tokio::spawn(c2);
        h2 = h2.ready().await?;
        let (response, _) =
            h2.send_request(Request::builder().uri("http://localhost/").body(())?, true)?;
        let response = response
            .await
            .context("first H2 request on retired worker")?;
        assert_eq!(response.status(), 200);
        let mut stream = response.into_body();
        let mut body = Vec::new();
        while let Some(chunk) = stream.data().await {
            let chunk = chunk.context("first H2 body on retired worker")?;
            stream.flow_control().release_capacity(chunk.len())?;
            body.extend_from_slice(&chunk);
        }
        assert_eq!(body, b"a");
        drop(stream);
        drop(h1);
        drop(h2);
        timeout(Duration::from_secs(2), d1).await???;
        timeout(Duration::from_secs(2), d2)
            .await??
            .context("H2 driver after retired worker completed first response")?;
        p.wait_for_workers(1, 3000).await?;
        let (mut fresh, conn) =
            crate::http1::handshake(TokioIo::new(TcpStream::connect(f.address()).await?)).await?;
        let driver = tokio::spawn(conn);
        assert_eq!(
            fresh
                .send_request(request("/"))
                .await?
                .into_body()
                .collect()
                .await?
                .to_bytes(),
            "b"
        );
        drop(fresh);
        driver.abort();
        p.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_seed_commits_atomically_and_never_overrides_managed_config() -> Result<()> {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let backend = tagged(b'A').await?;
    let f = Fixture::new(Protocol::Tcp, backend.address);
    let seed = f.dir.path().join("seed.toml");
    std::fs::copy(&f.path, &seed)?;
    std::fs::remove_file(&f.path)?;
    let occupied = TcpListener::bind(f.address()).await?;
    assert!(ProxyBuilder::new(&f.path)?
        .seed(&seed)
        .start()
        .await
        .is_err());
    assert!(
        !f.path.exists(),
        "failed startup published a partial config"
    );
    drop(occupied);
    let proxy = ProxyBuilder::new(&f.path)?.seed(&seed).start().await?;
    assert_eq!(
        std::fs::metadata(&f.path)?.permissions().mode() & 0o777,
        0o600
    );
    let mut io = TcpStream::connect(f.address()).await?;
    assert_eq!(io.read_u8().await?, b'A');
    drop(io);
    proxy.set_enabled("main/one".into(), false, 0).await?;
    proxy.shutdown().await;
    drop(proxy);
    std::fs::remove_file(&seed)?;
    let proxy = ProxyBuilder::new(&f.path)?.seed(&seed).start().await?;
    assert_eq!(proxy.snapshot().config.generation, 1);
    assert!(!proxy.snapshot().config.upstreams["main"].endpoints[0].enabled);
    proxy.shutdown().await;
    drop(proxy);
    let valid = f.config.encode()?;
    std::fs::write(&seed, valid)?;
    std::fs::write(&f.path, "invalid managed config")?;
    assert!(ProxyBuilder::new(&f.path)?
        .seed(&seed)
        .start()
        .await
        .is_err());
    assert_eq!(std::fs::read_to_string(&f.path)?, "invalid managed config");
    std::fs::remove_file(&f.path)?;
    symlink(f.dir.path().join("missing"), &f.path)?;
    assert!(ProxyBuilder::new(&f.path)?
        .seed(&seed)
        .start()
        .await
        .is_err());
    assert!(!f.dir.path().join("missing").exists());
    Ok(())
}
