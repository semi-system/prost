use super::*;
use prost_config::{ClientTls, PassthroughRoute, PassthroughSettings, ServerTls};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct TlsBackend {
    server: Server,
    accepted: Arc<AtomicUsize>,
    source: Arc<Mutex<Option<SocketAddr>>>,
}
async fn backend(tls: &ServerTls, label: u8, proxy: bool) -> Result<TlsBackend> {
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(tls, false)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let accepted = Arc::new(AtomicUsize::new(0));
    let source = Arc::new(Mutex::new(None));
    let task = {
        let accepted = accepted.clone();
        let source = source.clone();
        tokio::spawn(async move {
            while let Ok((mut io, peer)) = listener.accept().await {
                let acceptor = acceptor.clone();
                let accepted = accepted.clone();
                let source = source.clone();
                tokio::spawn(async move {
                    let work = async {
                        let source_address = if proxy {
                            let destination = io.local_addr()?;
                            prost_transport::read_proxy_addresses(
                                &mut io,
                                peer,
                                destination,
                                &prost_config::ProxyProtocol {
                                    trusted_peers: vec!["127.0.0.0/8".parse()?],
                                    timeout_ms: 1000,
                                },
                            )
                            .await?
                            .source
                        } else {
                            peer
                        };
                        let mut tls = acceptor.accept(io).await?;
                        accepted.fetch_add(1, Ordering::Relaxed);
                        *source.lock().unwrap() = Some(source_address);
                        tls.write_all(&[label]).await?;
                        let (mut r, mut w) = tokio::io::split(&mut tls);
                        tokio::io::copy(&mut r, &mut w).await?;
                        Ok::<_, anyhow::Error>(())
                    };
                    let _ = work.await;
                });
            }
        })
    };
    Ok(TlsBackend {
        server: Server { address, task },
        accepted,
        source,
    })
}
fn client(tls: &ServerTls, identity: bool, sni: bool) -> Result<tokio_rustls::TlsConnector> {
    let mut config = (*prost_transport::client_tls(Some(&ClientTls {
        server_name: "api.example.test".into(),
        ca_certificate: Some(tls.certificate.clone()),
        identity: identity.then(|| prost_config::ClientIdentity {
            certificate: tls.certificate.clone(),
            private_key: tls.private_key.clone(),
        }),
        policy: None,
    }))?)
    .clone();
    config.enable_sni = sni;
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}
async fn open(
    connector: &tokio_rustls::TlsConnector,
    address: SocketAddr,
    name: &str,
    label: u8,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let mut io = timeout(
        Duration::from_secs(3),
        connector.connect(
            name.to_owned().try_into()?,
            TcpStream::connect(address).await?,
        ),
    )
    .await??;
    assert_eq!(timeout(Duration::from_secs(2), io.read_u8()).await??, label);
    Ok(io)
}
fn settings(strict: bool) -> PassthroughSettings {
    PassthroughSettings {
        reject_unknown_sni: strict,
        server_names: vec!["fallback.other.test".into()],
        routes: vec![
            PassthroughRoute {
                server_names: vec!["*.example.test".into()],
                upstream: "green".into(),
            },
            PassthroughRoute {
                server_names: vec!["API.EXAMPLE.TEST.".into()],
                upstream: "main".into(),
            },
        ],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sni_routes_exact_wildcard_mtls_proxy_hot_apply_and_restart() -> Result<()> {
    let unused = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, unused.address);
    let tls = certificate_for(
        f.dir.path(),
        &[
            "api.example.test",
            "x.example.test",
            "example.test",
            "deep.x.example.test",
            "fallback.other.test",
            "unknown.test",
        ],
    )?;
    let blue = backend(&tls, b'B', false).await?;
    let mut mutual = tls.clone();
    mutual.client_ca = Some(tls.certificate.clone());
    let green = backend(&mutual, b'G', true).await?;
    let main = f.config.upstreams.get_mut("main").unwrap();
    main.endpoints[0].address = blue.server.address.to_string();
    main.idle_timeout_ms = 60000;
    main.health_interval_ms = 60000;
    let mut other = main.clone();
    other.endpoints[0].address = green.server.address.to_string();
    other.send_proxy_v2 = true;
    f.config.upstreams.insert("green".into(), other);
    f.config.listeners[0].tls_passthrough = true;
    f.config.listeners[0].tls_passthrough_settings = Some(settings(true));
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let plain = client(&tls, false, true)?;
    let authenticated = client(&tls, true, true)?;
    let mut old = open(&plain, f.address(), "api.example.test", b'B').await?;
    let mut wild = open(&authenticated, f.address(), "x.example.test", b'G').await?;
    assert_eq!(
        *green.source.lock().unwrap(),
        Some(wild.get_ref().0.local_addr()?)
    );
    roundtrip(&mut wild, b"opaque mTLS payload").await?;
    let fallback = open(&plain, f.address(), "fallback.other.test", b'B').await?;
    drop(fallback);
    let before = blue.accepted.load(Ordering::Relaxed) + green.accepted.load(Ordering::Relaxed);
    for name in ["unknown.test", "example.test", "deep.x.example.test"] {
        assert!(timeout(
            Duration::from_secs(3),
            plain.connect(
                name.to_owned().try_into()?,
                TcpStream::connect(f.address()).await?
            )
        )
        .await?
        .is_err());
    }
    assert!(timeout(
        Duration::from_secs(3),
        client(&tls, false, false)?.connect(
            "api.example.test".try_into()?,
            TcpStream::connect(f.address()).await?
        )
    )
    .await?
    .is_err());
    assert_eq!(
        blue.accepted.load(Ordering::Relaxed) + green.accepted.load(Ordering::Relaxed),
        before
    );
    let unauthenticated = plain
        .connect(
            "x.example.test".try_into()?,
            TcpStream::connect(f.address()).await?,
        )
        .await;
    if let Ok(mut io) = unauthenticated {
        assert!(timeout(Duration::from_secs(2), io.read_u8())
            .await?
            .is_err());
    }
    let mut next = p.snapshot().config.clone();
    next.listeners[0]
        .tls_passthrough_settings
        .as_mut()
        .unwrap()
        .routes[1]
        .upstream = "green".into();
    p.apply(next.encode()?, 0).await?;
    roundtrip(&mut old, b"old route remains pinned").await?;
    let mut new = open(&authenticated, f.address(), "api.example.test", b'G').await?;
    roundtrip(&mut new, b"new route").await?;
    let hash = p.snapshot().hash.clone();
    let mut invalid = p.snapshot().config.clone();
    invalid.listeners[0]
        .tls_passthrough_settings
        .as_mut()
        .unwrap()
        .routes[0]
        .upstream = "missing".into();
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    let mut invalid = p.snapshot().config.clone();
    let upstream = invalid.upstreams.get_mut("green").unwrap();
    upstream.balance = "consistent_hash".into();
    upstream.hash_key = Some("$http_x_tenant".into());
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    let mut invalid = p.snapshot().config.clone();
    invalid.upstreams.get_mut("green").unwrap().tls = Some(ClientTls {
        server_name: "api.example.test".into(),
        ca_certificate: None,
        identity: None,
        policy: None,
    });
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    assert_eq!(p.snapshot().hash, hash);
    assert_eq!(p.snapshot().config.generation, 1);
    drop(old);
    drop(wild);
    drop(new);
    p.shutdown().await;
    drop(p);
    let restarted = f.start().await;
    assert_eq!(restarted.snapshot().config.generation, 1);
    let mut restored = open(&authenticated, f.address(), "api.example.test", b'G').await?;
    roundtrip(&mut restored, b"persisted SNI route").await?;
    assert_eq!(
        metric(
            &restarted,
            "prost_tls_handshakes_total",
            &[("result", "success")]
        ),
        Some(0.),
        "TLS was not terminated at Prost"
    );
    drop(restored);
    restarted.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn permissive_sni_uses_default_for_unknown_missing_apex_and_deep_names() -> Result<()> {
    let unused = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, unused.address);
    let tls = certificate_for(
        f.dir.path(),
        &[
            "api.example.test",
            "x.example.test",
            "unknown.test",
            "example.test",
            "deep.x.example.test",
        ],
    )?;
    let blue = backend(&tls, b'B', false).await?;
    f.config.upstreams.get_mut("main").unwrap().endpoints[0].address =
        blue.server.address.to_string();
    f.config
        .upstreams
        .insert("green".into(), f.config.upstreams["main"].clone());
    f.config.listeners[0].tls_passthrough = true;
    f.config.listeners[0].tls_passthrough_settings = Some(settings(false));
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for name in ["unknown.test", "example.test", "deep.x.example.test"] {
        let mut io = open(&client(&tls, false, true)?, f.address(), name, b'B').await?;
        roundtrip(&mut io, b"fallback").await?;
    }
    let mut missing = open(
        &client(&tls, false, false)?,
        f.address(),
        "api.example.test",
        b'B',
    )
    .await?;
    roundtrip(&mut missing, b"no SNI fallback").await?;
    drop(missing);
    p.set_enabled("green/one".into(), false, 0).await?;
    let before = blue.accepted.load(Ordering::Relaxed);
    assert!(
        timeout(
            Duration::from_secs(3),
            client(&tls, false, true)?.connect(
                "x.example.test".try_into()?,
                TcpStream::connect(f.address()).await?
            )
        )
        .await?
        .is_err(),
        "matched unavailable route must not fall through to default upstream"
    );
    assert_eq!(blue.accepted.load(Ordering::Relaxed), before);
    let mut fallback = open(
        &client(&tls, false, true)?,
        f.address(),
        "unknown.test",
        b'B',
    )
    .await?;
    roundtrip(
        &mut fallback,
        b"default is independent of routed upstream health",
    )
    .await?;
    drop(fallback);
    p.shutdown().await;
    Ok(())
}
