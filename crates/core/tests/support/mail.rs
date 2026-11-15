use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

// An independent backend decoder: require binary v2 before any TLS/application
// byte, and expose both original endpoints to the test service.
async fn v2(io: &mut TcpStream) -> Result<(SocketAddr, SocketAddr)> {
    let mut h = [0u8; 16];
    io.read_exact(&mut h).await?;
    anyhow::ensure!(
        &h[..12] == b"\r\n\r\n\0\r\nQUIT\n" && h[12] == 0x21,
        "expected PROXY v2"
    );
    let mut data = vec![0u8; u16::from_be_bytes([h[14], h[15]]) as usize];
    anyhow::ensure!(data.len() <= 36, "unexpected TLV in fixture");
    io.read_exact(&mut data).await?;
    let pair = match (h[13], data.len()) {
        (0x11, 12) => (
            SocketAddr::new(
                std::net::Ipv4Addr::from(<[u8; 4]>::try_from(&data[..4])?).into(),
                u16::from_be_bytes([data[8], data[9]]),
            ),
            SocketAddr::new(
                std::net::Ipv4Addr::from(<[u8; 4]>::try_from(&data[4..8])?).into(),
                u16::from_be_bytes([data[10], data[11]]),
            ),
        ),
        (0x21, 36) => (
            SocketAddr::new(
                std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&data[..16])?).into(),
                u16::from_be_bytes([data[32], data[33]]),
            ),
            SocketAddr::new(
                std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&data[16..32])?).into(),
                u16::from_be_bytes([data[34], data[35]]),
            ),
        ),
        _ => anyhow::bail!("invalid TCP address family/length"),
    };
    Ok(pair)
}
fn client(tls: &prost_config::ServerTls, identity: bool) -> prost_config::ClientTls {
    prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate.clone()),
        identity: identity.then(|| prost_config::ClientIdentity {
            certificate: tls.certificate.clone(),
            private_key: tls.private_key.clone(),
        }),
    }
}
fn ingress(f: &mut Fixture) {
    f.config.listeners[0].proxy_protocol = Some(prost_config::ProxyProtocol {
        trusted_peers: vec!["127.0.0.1/32".parse().unwrap()],
        timeout_ms: 500,
    });
}
async fn incoming(address: SocketAddr, source: &str) -> Result<TcpStream> {
    let mut io = TcpStream::connect(address).await?;
    io.write_all(format!("PROXY TCP4 {source} 203.0.113.10 12345 10443\r\n").as_bytes())
        .await?;
    Ok(io)
}
async fn wait_health(p: &RunningProxy, healthy: bool) -> Result<()> {
    timeout(Duration::from_secs(4), async {
        loop {
            if p.status()["upstreams"]["main"][0]["healthy"] == healthy {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn proxy_v2_before_mtls_h2_pools_keep_client_identity_and_rotate() -> Result<()> {
    let seed = echo().await;
    let mut f = Fixture::new(Protocol::Http, seed.address);
    f.config.runtime.max_proxy_connections = 2;
    let mut tls = certificate(f.dir.path())?;
    tls.client_ca = Some(tls.certificate.clone());
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, true)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let sequence = Arc::new(AtomicUsize::new(0));
    let rejected = Arc::new(AtomicUsize::new(0));
    let task = {
        let sequence = sequence.clone();
        let rejected = rejected.clone();
        tokio::spawn(async move {
            while let Ok((mut io, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let sequence = sequence.clone();
                let rejected = rejected.clone();
                tokio::spawn(async move {
                    let Ok((src, dst)) = v2(&mut io).await else {
                        return;
                    };
                    let io = match acceptor.accept(io).await {
                        Ok(io) => io,
                        Err(_) => {
                            rejected.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };
                    let number = sequence.fetch_add(1, Ordering::Relaxed);
                    let metadata = format!(
                        "{src}|{dst}|{number}|{:?}",
                        io.get_ref().1.protocol_version()
                    );
                    let service = service_fn(move |request: Request<Incoming>| {
                        let metadata = metadata.clone();
                        async move {
                            request.into_body().collect().await.unwrap();
                            Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(metadata))))
                        }
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                });
            }
        })
    };
    let _backend = Server { address, task };
    ingress(&mut f);
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.send_proxy_v2 = true;
    u.health_interval_ms = 60000;
    u.http2 = true;
    u.tls = Some(client(&tls, true));
    u.health_check.protocol = prost_config::HealthProtocol::Http;
    u.health_check.path = Some("/health".into());
    u.tls.as_mut().unwrap().policy = Some(prost_config::TlsPolicy {
        min_version: prost_config::TlsMinimum::Tls13,
        cipher_suites: vec!["TLS13_AES_128_GCM_SHA256".into()],
        session_resumption: false,
    });
    u.endpoints[0].address = address.to_string();
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let (mut a, ca) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(incoming(f.address(), "198.51.100.1").await?),
    )
    .await?;
    let (mut b, cb) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(incoming(f.address(), "198.51.100.2").await?),
    )
    .await?;
    let da = tokio::spawn(ca);
    let db = tokio::spawn(cb);
    let request = || {
        Request::builder()
            .uri("http://localhost/")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let body = |r: Response<Incoming>| async move {
        let data = r.into_body().collect().await?.to_bytes();
        Ok::<_, anyhow::Error>(String::from_utf8(data.to_vec())?)
    };
    let first = body(a.send_request(request()).await?).await?;
    let second = body(b.send_request(request()).await?).await?;
    assert!(
        first.starts_with("198.51.100.1:12345|203.0.113.10:10443|"),
        "{first}"
    );
    assert!(
        second.starts_with("198.51.100.2:12345|203.0.113.10:10443|"),
        "{second}"
    );
    assert!(first.ends_with("Some(TLSv1_3)"));
    assert_ne!(first.split('|').nth(2), second.split('|').nth(2));
    assert_eq!(body(a.send_request(request()).await?).await?, first);
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
    p.apply(next.encode()?, 0).await?;
    assert_eq!(
        body(a.send_request(request()).await?).await?,
        first,
        "compatible hot updates should retain scoped pool"
    );
    let mut ipv6 = TcpStream::connect(f.address()).await?;
    ipv6.write_all(b"PROXY TCP6 2001:db8::1 2001:db8::2 54321 10443\r\n")
        .await?;
    let (mut c, cc) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(ipv6)).await?;
    let dc = tokio::spawn(cc);
    let overloaded = c.send_request(request()).await?;
    assert_eq!(overloaded.status(), 503);
    overloaded.into_body().collect().await?;
    assert_eq!(
        p.status()["upstreams"]["main"][0]["healthy"],
        true,
        "local scoped capacity is not a backend failure"
    );
    drop(b);
    db.abort();
    timeout(Duration::from_secs(2), async {
        loop {
            let response = c.send_request(request()).await?;
            if response.status() == 200 {
                let value = body(response).await?;
                assert!(
                    value.starts_with("[2001:db8::1]:54321|[2001:db8::2]:10443|"),
                    "{value}"
                );
                return Ok::<_, anyhow::Error>(());
            }
            response.into_body().collect().await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let mut next = p.snapshot().config.clone();
    next.upstreams
        .get_mut("main")
        .unwrap()
        .tls
        .as_mut()
        .unwrap()
        .identity = None;
    p.apply(next.encode()?, 1).await?;
    assert_eq!(a.send_request(request()).await?.status(), 503);
    timeout(Duration::from_secs(2), async {
        while rejected.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    drop(a);
    da.abort();
    drop(c);
    dc.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smtp_starttls_health_rejects_silent_bad_capability_and_post_tls_stalls_and_restarts(
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let seed = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, seed.address);
    let mut tls = certificate(f.dir.path())?;
    tls.client_ca = Some(tls.certificate.clone());
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, false)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mode = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let task = {
        let mode = mode.clone();
        let successes = successes.clone();
        tokio::spawn(async move {
            while let Ok((mut io, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let mode = mode.load(Ordering::Relaxed);
                let successes = successes.clone();
                tokio::spawn(async move {
                    let work = async {
                        v2(&mut io).await?;
                        if mode == 1 {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            return Ok::<_, anyhow::Error>(());
                        }
                        io.write_all(if mode == 2 {
                            b"554 unavailable\r\n"
                        } else {
                            b"220 mail ESMTP\r\n"
                        })
                        .await?;
                        let mut io = BufReader::new(io);
                        let mut line = String::new();
                        io.read_line(&mut line).await?;
                        anyhow::ensure!(line.starts_with("EHLO "), "expected EHLO");
                        io.write_all(if mode == 3 {
                            b"250 mail\r\n"
                        } else {
                            b"250-mail\r\n250 STARTTLS\r\n"
                        })
                        .await?;
                        line.clear();
                        io.read_line(&mut line).await?;
                        anyhow::ensure!(line == "STARTTLS\r\n", "expected STARTTLS");
                        io.write_all(b"220 ready\r\n").await?;
                        let mut io = BufReader::new(acceptor.accept(io.into_inner()).await?);
                        line.clear();
                        io.read_line(&mut line).await?;
                        anyhow::ensure!(line.starts_with("EHLO "), "EHLO after TLS");
                        if mode == 4 {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            return Ok(());
                        }
                        io.write_all(b"250 mail secured\r\n").await?;
                        line.clear();
                        io.read_line(&mut line).await?;
                        anyhow::ensure!(line == "QUIT\r\n", "QUIT after TLS");
                        io.write_all(b"221 bye\r\n").await?;
                        successes.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    };
                    let _ = timeout(Duration::from_secs(6), work).await;
                });
            }
        })
    };
    let _backend = Server { address, task };
    let u = f.config.upstreams.get_mut("main").unwrap();
    u.endpoints[0].address = address.to_string();
    u.send_proxy_v2 = true;
    u.health_check.protocol = prost_config::HealthProtocol::SmtpStarttls;
    u.health_check.timeout_ms = 400;
    u.health_check.tls = Some(client(&tls, true));
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    timeout(Duration::from_secs(3), async {
        while successes.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    for fault in [1, 2, 3, 4] {
        mode.store(fault, Ordering::Relaxed);
        wait_health(&p, false).await?;
        mode.store(0, Ordering::Relaxed);
        wait_health(&p, true).await?;
    }
    let mut bad = p.snapshot().config.clone();
    bad.upstreams
        .get_mut("main")
        .unwrap()
        .health_check
        .tls
        .as_mut()
        .unwrap()
        .server_name = "wrong.invalid".into();
    p.apply(bad.encode()?, 0).await?;
    wait_health(&p, false).await?;
    let mut good = p.snapshot().config.clone();
    good.upstreams
        .get_mut("main")
        .unwrap()
        .health_check
        .tls
        .as_mut()
        .unwrap()
        .server_name = "localhost".into();
    p.apply(good.encode()?, 1).await?;
    wait_health(&p, true).await?;
    std::fs::remove_file(&tls.certificate)?;
    std::fs::remove_file(&tls.private_key)?;
    p.shutdown().await;
    let before = successes.load(Ordering::Relaxed);
    drop(p);
    let p = f.start().await;
    timeout(Duration::from_secs(3), async {
        while successes.load(Ordering::Relaxed) == before {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    wait_health(&p, true).await?;
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_ip_tcp_limit_precedes_tls_and_survives_hot_updates() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    ingress(&mut f);
    f.config.listeners[0].max_connections_per_ip = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut first = incoming(f.address(), "198.51.100.1").await?;
    roundtrip(&mut first, b"held").await?;
    let mut denied = incoming(f.address(), "198.51.100.1").await?;
    assert_eq!(
        timeout(Duration::from_secs(1), denied.read_u8())
            .await?
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    let mut other = incoming(f.address(), "198.51.100.2").await?;
    roundtrip(&mut other, b"independent").await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].max_connections_per_ip = 0;
    p.apply(next.encode()?, 0).await?;
    let mut second = incoming(f.address(), "198.51.100.1").await?;
    roundtrip(&mut second, b"unlimited but tracked").await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].max_connections_per_ip = 2;
    p.apply(next.encode()?, 1).await?;
    let mut denied = incoming(f.address(), "198.51.100.1").await?;
    assert!(timeout(Duration::from_secs(1), denied.read_u8())
        .await?
        .is_err());
    drop(first);
    drop(second);
    drop(other);
    timeout(Duration::from_secs(2), async {
        while p.status()["connections"] != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut io = incoming(f.address(), "198.51.100.1").await?;
    roundtrip(&mut io, b"released").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passthrough_strict_sni_bounds_clienthello_and_replays_bytes() -> Result<()> {
    let seed = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, seed.address);
    let tls = certificate_for(f.dir.path(), &["mail.test", "unknown.test"])?;
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, false)?);
    let accepted = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = {
        let accepted = accepted.clone();
        tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let accepted = accepted.clone();
                tokio::spawn(async move {
                    if let Ok(mut io) = acceptor.accept(io).await {
                        accepted.fetch_add(1, Ordering::Relaxed);
                        let mut buffer = [0u8; 1024];
                        while let Ok(n) = io.read(&mut buffer).await {
                            if n == 0 {
                                break;
                            }
                            if io.write_all(&buffer[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        })
    };
    let _backend = Server { address, task };
    f.config.upstreams.get_mut("main").unwrap().endpoints[0].address = address.to_string();
    f.config.listeners[0].tls_passthrough = true;
    f.config.listeners[0].tls_passthrough_settings = Some(prost_config::PassthroughSettings {
        client_hello_timeout_ms: 100,
        reject_unknown_sni: true,
        server_names: vec!["mail.test".into()],
        ..Default::default()
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut config = client(&tls, false);
    config.server_name = "mail.test".into();
    let good = prost_transport::client_tls(Some(&config))?;
    let mut io = tokio_rustls::TlsConnector::from(good.clone())
        .connect(
            "mail.test".try_into()?,
            TcpStream::connect(f.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"opaque and byte-exact").await?;
    drop(io);
    let before = accepted.load(Ordering::Relaxed);
    assert!(tokio_rustls::TlsConnector::from(good.clone())
        .connect(
            "unknown.test".try_into()?,
            TcpStream::connect(f.address()).await?
        )
        .await
        .is_err());
    let mut no_sni = (*good).clone();
    no_sni.enable_sni = false;
    assert!(tokio_rustls::TlsConnector::from(Arc::new(no_sni))
        .connect(
            "mail.test".try_into()?,
            TcpStream::connect(f.address()).await?
        )
        .await
        .is_err());
    for prefix in [&b""[..], &b"\x16\x03\x03\x00\x10\x01"[..]] {
        let mut stalled = TcpStream::connect(f.address()).await?;
        stalled.write_all(prefix).await?;
        assert!(timeout(Duration::from_secs(2), stalled.read_u8())
            .await?
            .is_err());
    }
    assert_eq!(
        accepted.load(Ordering::Relaxed),
        before,
        "rejected ClientHello must not establish backend TLS"
    );
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls12_tls13_cipher_policy_and_tcp_handshake_admission() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    let mut tls = certificate(f.dir.path())?;
    tls.policy = Some(prost_config::TlsPolicy {
        cipher_suites: vec![
            "TLS13_AES_128_GCM_SHA256".into(),
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256".into(),
        ],
        session_resumption: false,
        ..Default::default()
    });
    f.config.listeners[0].tls = Some(tls.clone());
    f.config.listeners[0].tls_handshake_timeout_ms = 300;
    f.config.listeners[0].max_connections_per_ip = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let mut store = rustls::RootCertStore::empty();
        for cert in rustls_pemfile_for_test(&tls.certificate)? {
            store.add(cert)?;
        }
        let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[version])?
        .with_root_certificates(store)
        .with_no_client_auth();
        let mut io = tokio_rustls::TlsConnector::from(Arc::new(cfg))
            .connect(
                "localhost".try_into()?,
                TcpStream::connect(f.address()).await?,
            )
            .await?;
        assert_eq!(io.get_ref().1.protocol_version(), Some(version.version));
        assert!(format!(
            "{:?}",
            io.get_ref().1.negotiated_cipher_suite().unwrap().suite()
        )
        .contains("AES_128_GCM"));
        roundtrip(&mut io, b"TLS policy").await?;
        drop(io);
        timeout(Duration::from_secs(1), async {
            while p.status()["connections"] != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    // Legacy ClientHello records must receive a fatal alert. The provider test
    // separately asserts the exact supported protocol versions and suite families.
    for minor in [1u8, 2] {
        let mut hello = vec![3, minor];
        hello.extend_from_slice(&[0u8; 32]);
        hello.extend_from_slice(&[0, 0, 2, 0xc0, 0x09, 1, 0]);
        let mut handshake = vec![1, 0, 0, hello.len() as u8];
        handshake.extend(hello);
        let mut record = vec![22, 3, 1, 0, handshake.len() as u8];
        record.extend(handshake);
        let mut io = TcpStream::connect(f.address()).await?;
        io.write_all(&record).await?;
        let mut alert = [0u8; 7];
        timeout(Duration::from_secs(1), io.read_exact(&mut alert)).await??;
        assert_eq!(alert[0], 21);
        assert_eq!(
            alert[5], 2,
            "legacy TLS must receive a fatal alert: {alert:?}"
        );
        drop(io);
        timeout(Duration::from_secs(1), async {
            while p.status()["connections"] != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
    }
    let first = TcpStream::connect(f.address()).await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let mut second = TcpStream::connect(f.address()).await?;
    assert!(
        timeout(Duration::from_millis(200), second.read_u8())
            .await?
            .is_err(),
        "second peer must be rejected before waiting for ClientHello"
    );
    let mut first = first;
    assert!(timeout(Duration::from_secs(1), first.read_u8())
        .await?
        .is_err());
    drop(first);
    let mut next = p.snapshot().config.clone();
    next.listeners[0]
        .tls
        .as_mut()
        .unwrap()
        .policy
        .as_mut()
        .unwrap()
        .min_version = prost_config::TlsMinimum::Tls13;
    p.apply(next.encode()?, 0).await?;
    let mut store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile_for_test(&tls.certificate)? {
        store.add(cert)?;
    }
    let old = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS12])?
    .with_root_certificates(store)
    .with_no_client_auth();
    assert!(tokio_rustls::TlsConnector::from(Arc::new(old))
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(f.address()).await?
        )
        .await
        .is_err());
    p.shutdown().await;
    Ok(())
}

fn rustls_pemfile_for_test(
    path: &std::path::Path,
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    // rcgen's test fixture uses PEM; read through the stable pki-types PEM API.
    use rustls::pki_types::pem::PemObject;
    Ok(rustls::pki_types::CertificateDer::pem_file_iter(path)?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_rate_is_per_ip_and_retains_debt_across_apply() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    ingress(&mut f);
    f.config.listeners[0].plugins.push(builtin("connection_rate",serde_json::json!({"connections_per_second":1,"burst":2,"key":"$remote_addr","max_keys":8,"idle_timeout_ms":30000}))?);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for _ in 0..2 {
        let mut io = incoming(f.address(), "198.51.100.1").await?;
        roundtrip(&mut io, b"rate").await?;
    }
    let mut io = incoming(f.address(), "198.51.100.1").await?;
    assert!(timeout(Duration::from_secs(1), io.read_u8())
        .await?
        .is_err());
    let mut other = incoming(f.address(), "198.51.100.2").await?;
    roundtrip(&mut other, b"independent").await?;
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].weight = 2;
    p.apply(next.encode()?, 0).await?;
    let mut io = incoming(f.address(), "198.51.100.1").await?;
    assert!(timeout(Duration::from_secs(1), io.read_u8())
        .await?
        .is_err());
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let mut io = incoming(f.address(), "198.51.100.1").await?;
    roundtrip(&mut io, b"refilled").await?;
    drop(io);
    drop(other);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tcp_half_close_deadline_drains_then_reclaims_a_silent_peer() -> Result<()> {
    let (mut client, proxy_client) = tokio::io::duplex(64);
    let (proxy_backend, mut backend) = tokio::io::duplex(64);
    let relay = tokio::spawn(prost_transport::relay_with_timeouts(
        Box::new(proxy_client),
        Box::new(proxy_backend),
        Duration::from_secs(5),
        Some(Duration::from_millis(100)),
    ));
    client.shutdown().await?;
    assert_eq!(
        backend.read_u8().await.unwrap_err().kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    backend.write_all(b"final bytes").await?;
    let mut data = [0u8; 11];
    client.read_exact(&mut data).await?;
    assert_eq!(&data, b"final bytes");
    assert!(timeout(Duration::from_secs(1), relay)
        .await??
        .unwrap_err()
        .to_string()
        .contains("half-close"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imap_pop3_banner_and_starttls_health_use_proxy_and_verified_mtls() -> Result<()> {
    use prost_config::HealthProtocol as H;
    use tokio::io::{AsyncBufReadExt, BufReader};
    for protocol in [H::Imap, H::ImapStarttls, H::Pop3, H::Pop3Starttls] {
        let seed = echo().await;
        let mut f = Fixture::new(Protocol::Tcp, seed.address);
        let mut tls = certificate(f.dir.path())?;
        tls.client_ca = Some(tls.certificate.clone());
        let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, false)?);
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let mode = Arc::new(AtomicUsize::new(0));
        let checked = Arc::new(AtomicUsize::new(0));
        let starttls = matches!(protocol, H::ImapStarttls | H::Pop3Starttls);
        let imap = matches!(protocol, H::Imap | H::ImapStarttls);
        let task = {
            let mode = mode.clone();
            let checked = checked.clone();
            tokio::spawn(async move {
                while let Ok((mut io, _)) = listener.accept().await {
                    let acceptor = acceptor.clone();
                    let mode = mode.load(Ordering::Relaxed);
                    let checked = checked.clone();
                    tokio::spawn(async move {
                        let work = async {
                            v2(&mut io).await?;
                            let io: prost_transport::BoxStream = if starttls {
                                Box::new(io)
                            } else {
                                Box::new(acceptor.accept(io).await?)
                            };
                            let mut io = BufReader::new(io);
                            io.write_all(if mode != 0 {
                                b"-ERR broken\r\n"
                            } else if imap {
                                b"* OK mail service IMAP ready\r\n"
                            } else {
                                b"+OK mail service POP3 ready\r\n"
                            })
                            .await?;
                            if mode != 0 {
                                return Ok::<_, anyhow::Error>(());
                            }
                            if !starttls {
                                checked.fetch_add(1, Ordering::Relaxed);
                                return Ok(());
                            }
                            let mut command = String::new();
                            io.read_line(&mut command).await?;
                            anyhow::ensure!(
                                command
                                    == if imap {
                                        "a001 CAPABILITY\r\n"
                                    } else {
                                        "CAPA\r\n"
                                    },
                                "capability command"
                            );
                            io.write_all(if imap {
                                b"* CAPABILITY IMAP4rev1 STARTTLS\r\na001 OK done\r\n"
                            } else {
                                b"+OK capabilities\r\nSTLS\r\n.\r\n"
                            })
                            .await?;
                            command.clear();
                            io.read_line(&mut command).await?;
                            anyhow::ensure!(
                                command
                                    == if imap {
                                        "a002 STARTTLS\r\n"
                                    } else {
                                        "STLS\r\n"
                                    },
                                "upgrade command"
                            );
                            io.write_all(if imap {
                                b"a002 OK ready\r\n"
                            } else {
                                b"+OK ready\r\n"
                            })
                            .await?;
                            let mut io = BufReader::new(acceptor.accept(io.into_inner()).await?);
                            command.clear();
                            io.read_line(&mut command).await?;
                            anyhow::ensure!(
                                command == if imap { "a003 NOOP\r\n" } else { "NOOP\r\n" },
                                "command after TLS"
                            );
                            io.write_all(if imap {
                                b"* OK secured\r\na003 OK done\r\n"
                            } else {
                                b"+OK secured\r\n"
                            })
                            .await?;
                            if !imap {
                                command.clear();
                                io.read_line(&mut command).await?;
                                anyhow::ensure!(command == "QUIT\r\n", "POP3 QUIT");
                                io.write_all(b"+OK bye\r\n").await?;
                            }
                            checked.fetch_add(1, Ordering::Relaxed);
                            Ok(())
                        };
                        let _ = timeout(Duration::from_secs(2), work).await;
                    });
                }
            })
        };
        let _backend = Server { address, task };
        let u = f.config.upstreams.get_mut("main").unwrap();
        u.endpoints[0].address = address.to_string();
        u.send_proxy_v2 = true;
        u.health_check.protocol = protocol;
        u.health_check.timeout_ms = 500;
        if starttls {
            u.health_check.tls = Some(client(&tls, true));
        } else {
            u.tls = Some(client(&tls, true));
        }
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        timeout(Duration::from_secs(3), async {
            while checked.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        wait_health(&p, true).await?;
        mode.store(1, Ordering::Relaxed);
        wait_health(&p, false).await?;
        mode.store(0, Ordering::Relaxed);
        wait_health(&p, true).await?;
        p.shutdown().await;
    }
    Ok(())
}
