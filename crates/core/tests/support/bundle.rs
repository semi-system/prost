use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn policy_backend() -> Server {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let calls = Arc::new(std::sync::Mutex::new(std::collections::HashMap::<
        String,
        usize,
    >::new()));
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            let calls = calls.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let calls = calls.clone();
                    async move {
                        let path = request.uri().path().to_owned();
                        request.into_body().collect().await.unwrap();
                        let n = {
                            let mut calls = calls.lock().unwrap();
                            let n = calls.entry(path.clone()).or_default();
                            *n += 1;
                            *n
                        };
                        let body: http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible> =
                            if path == "/trailers" {
                                let mut trailers = http::HeaderMap::new();
                                trailers.insert("x-end", http::HeaderValue::from_static("done"));
                                StreamBody::new(futures_util::stream::iter(vec![
                                    Ok::<_, Infallible>(Frame::data(Bytes::from(vec![
                                        b'a';
                                        32768
                                    ]))),
                                    Ok(Frame::trailers(trailers)),
                                ]))
                                .boxed_unsync()
                            } else {
                                Full::new(Bytes::from(vec![b'a'; 32768]))
                                    .map_err(|e| match e {})
                                    .boxed_unsync()
                            };
                        let mut response = Response::builder()
                            .header("content-type", "text/plain")
                            .header("x-count", n.to_string())
                            .header(
                                "cache-control",
                                if path == "/private" {
                                    "private,max-age=60"
                                } else {
                                    "public,max-age=60"
                                },
                            )
                            .body(body)
                            .unwrap();
                        if (path == "/retry" && n < 3) || path == "/never" {
                            *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
                        }
                        if path == "/vary" {
                            response
                                .headers_mut()
                                .insert("vary", http::HeaderValue::from_static("x-user"));
                        }
                        if path == "/cookie" {
                            response.headers_mut().insert(
                                "set-cookie",
                                http::HeaderValue::from_static("session=secret"),
                            );
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
    Server { address, task }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gzip_brotli_cache_privacy_retry_and_trailers() -> Result<()> {
    let backend = policy_backend().await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].http2_settings.stream_write_timeout_ms = 1000;
    f.config.upstreams.get_mut("main").unwrap().http2 = true;
    f.config.upstreams.get_mut("main").unwrap().http = Some(prost_config::HttpPolicy {
        compression: Some(Default::default()),
        cache: Some(Default::default()),
        retry: Some(prost_config::Retry {
            max_attempts: 3,
            ..Default::default()
        }),
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(conn);
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/unseen")
                .header("cache-control", "only-if-cached")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 504);
    response.into_body().collect().await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/unseen")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.headers()["x-count"], "1");
    response.into_body().collect().await?;
    for encoding in ["gzip", "br"] {
        for _ in 0..2 {
            let response = client
                .send_request(
                    Request::builder()
                        .uri("http://localhost/cache")
                        .header("accept-encoding", encoding)
                        .body(Full::new(Bytes::new()))?,
                )
                .await?;
            assert_eq!(
                response.headers()["x-count"],
                if encoding == "gzip" { "1" } else { "2" }
            );
            assert_eq!(response.headers()["content-encoding"], encoding);
            assert!(response.headers()["vary"]
                .to_str()?
                .to_ascii_lowercase()
                .contains("accept-encoding"));
            let bytes = response.into_body().collect().await?.to_bytes();
            let mut decoded = Vec::new();
            if encoding == "gzip" {
                async_compression::tokio::bufread::GzipDecoder::new(bytes.as_ref())
                    .read_to_end(&mut decoded)
                    .await?;
            } else {
                async_compression::tokio::bufread::BrotliDecoder::new(bytes.as_ref())
                    .read_to_end(&mut decoded)
                    .await?;
            }
            assert_eq!(decoded, vec![b'a'; 32768]);
        }
    }
    for path in ["/private", "/vary", "/cookie"] {
        for expected in ["1", "2"] {
            let response = client
                .send_request(
                    Request::builder()
                        .uri(format!("http://localhost{path}"))
                        .body(Full::new(Bytes::new()))?,
                )
                .await?;
            assert_eq!(response.headers()["x-count"], expected);
            response.into_body().collect().await?;
        }
    }
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/retry")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["x-count"], "3");
    response.into_body().collect().await?;
    let response = client
        .send_request(
            Request::builder()
                .method("POST")
                .uri("http://localhost/never")
                .body(Full::new(Bytes::from_static(b"write")))?,
        )
        .await?;
    assert_eq!(response.status(), 503);
    assert_eq!(response.headers()["x-count"], "1");
    response.into_body().collect().await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/never")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 503);
    assert_eq!(response.headers()["x-count"], "4");
    response.into_body().collect().await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/trailers")
                .header("accept-encoding", "gzip")
                .header("te", "trailers")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.headers()["content-encoding"], "gzip");
    let collected = response.into_body().collect().await?;
    assert_eq!(collected.trailers().unwrap()["x-end"], "done");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn proxy_protocol_v1_v2_trust_and_source_acl() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.listeners[0].proxy_protocol = Some(prost_config::ProxyProtocol {
        trusted_peers: vec!["127.0.0.0/8".parse()?],
        timeout_ms: 100,
    });
    f.config.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"allow":["203.0.113.5/32"]}),
    )?);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut io = TcpStream::connect(f.address()).await?;
    io.write_all(b"PROXY TCP4 203.0.113.5 192.0.2.1 12345 443\r\n")
        .await?;
    roundtrip(&mut io, b"hello").await?;
    drop(io);
    let mut io = TcpStream::connect(f.address()).await?;
    let mut v2 = b"\r\n\r\n\0\r\nQUIT\n".to_vec();
    v2.extend_from_slice(&[
        0x21, 0x11, 0, 12, 203, 0, 113, 5, 192, 0, 2, 1, 0x30, 0x39, 1, 0xbb,
    ]);
    io.write_all(&v2).await?;
    roundtrip(&mut io, b"binary").await?;
    drop(io);
    let mut next = p.snapshot().config.clone();
    next.listeners[0]
        .proxy_protocol
        .as_mut()
        .unwrap()
        .trusted_peers = vec!["192.0.2.0/24".parse()?];
    p.apply(next.encode()?, 0).await?;
    let mut rejected = TcpStream::connect(f.address()).await?;
    assert!(timeout(Duration::from_secs(2), rejected.read_u8())
        .await?
        .is_err());
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mtls_upstream_and_opaque_tls_passthrough() -> Result<()> {
    let unused = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, unused.address);
    let tls = certificate(f.dir.path())?;
    let mut mutual = tls.clone();
    mutual.client_ca = Some(tls.certificate.clone());
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&mutual, false)?);
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let address = l.local_addr()?;
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            let acceptor = acceptor.clone();
            let count = count.clone();
            tokio::spawn(async move {
                if let Ok(mut io) = acceptor.accept(io).await {
                    count.fetch_add(1, Ordering::Relaxed);
                    let (mut r, mut w) = tokio::io::split(&mut io);
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                }
            });
        }
    });
    let _backend = Server { address, task };
    let identity = prost_config::ClientIdentity {
        certificate: tls.certificate.clone(),
        private_key: tls.private_key.clone(),
    };
    let config = prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate.clone()),
        identity: Some(identity),
    };
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.endpoints[0].address = address.to_string();
    u.tls = Some(config.clone());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut io = TcpStream::connect(f.address()).await?;
    roundtrip(&mut io, b"mutual").await?;
    drop(io);
    assert!(accepted.load(Ordering::Relaxed) > 0);
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().tls = None;
    next.listeners[0].tls_passthrough = true;
    p.apply(next.encode()?, 0).await?;
    let client = prost_transport::client_tls(Some(&config))?;
    let name = rustls::pki_types::ServerName::try_from("localhost")?;
    let mut io = tokio_rustls::TlsConnector::from(client)
        .connect(name, TcpStream::connect(f.address()).await?)
        .await?;
    roundtrip(&mut io, b"opaque TLS with client certificate").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn h2_zero_window_resets_only_stalled_stream() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.listeners[0].http2_settings.stream_write_timeout_ms = 80;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut client, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(0)
        .handshake(TokioIo::new(TcpStream::connect(f.address()).await?))
        .await?;
    let driver = tokio::spawn(conn);
    let response = client
        .send_request(
            Request::builder()
                .uri("http://localhost/hold")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert!(
        timeout(Duration::from_secs(2), response.into_body().collect())
            .await?
            .is_err()
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
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
