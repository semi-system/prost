use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct Sources {
    server: Server,
    files: Arc<Mutex<std::collections::HashMap<String, Bytes>>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
async fn sources(cert: &prost_config::ServerTls) -> Result<Sources> {
    let files = Arc::new(Mutex::new(std::collections::HashMap::from([
        (
            "/cert.pem".into(),
            Bytes::from(std::fs::read(&cert.certificate)?),
        ),
        (
            "/key.pem".into(),
            Bytes::from(std::fs::read(&cert.private_key)?),
        ),
        ("/bad".into(), Bytes::from_static(b"invalid PEM")),
    ])));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = {
        let files = files.clone();
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            while let Ok((io, _)) = listener.accept().await {
                let files = files.clone();
                let entered = entered.clone();
                let release = release.clone();
                let calls = calls.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let files = files.clone();
                        let entered = entered.clone();
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.fetch_add(1, Ordering::Relaxed);
                            if request.uri().path() == "/gated-cert.pem" {
                                entered.notify_one();
                                release.notified().await;
                            }
                            let bytes = files.lock().unwrap().get(request.uri().path()).cloned();
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(if bytes.is_some() { 200 } else { 404 })
                                    .body(Full::new(bytes.unwrap_or_default()))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                });
            }
        })
    };
    Ok(Sources {
        server: Server { address, task },
        files,
        calls,
        entered,
        release,
    })
}
fn file(path: &std::path::Path) -> PathBuf {
    format!("file://{}", path.display())
        .replace(' ', "%20")
        .into()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_file_http_sources_dedup_rotate_rollback_and_restart_offline() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    let first = certificate(&f.dir.path().join("first cert"))?;
    let second = certificate(&f.dir.path().join("second"))?;
    let origin = sources(&first).await?;
    let url = format!("http://{}", origin.server.address);
    let mut initial = first.clone();
    initial.certificate = file(&first.certificate);
    initial.private_key = file(&first.private_key);
    initial.certificates.push(prost_config::NamedCertificate {
        server_names: vec!["localhost".into()],
        certificate: format!("{url}/cert.pem").into(),
        private_key: format!("{url}/key.pem").into(),
    });
    f.config.listeners[0].tls = Some(initial.clone());
    let mut mutual = f.config.listeners[0].clone();
    mutual.id = "mutual".into();
    mutual.address = free_address();
    mutual.tls.as_mut().unwrap().client_ca = Some(format!("{url}/cert.pem").into());
    f.config.listeners.push(mutual);
    let mut identity = f.config.upstreams["main"].clone();
    identity.health_interval_ms = 60000;
    let client_tls = prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        ca_certificate: Some(format!("{url}/cert.pem").into()),
        identity: Some(prost_config::ClientIdentity {
            certificate: format!("{url}/cert.pem").into(),
            private_key: format!("{url}/key.pem").into(),
        }),
    };
    identity.tls = Some(client_tls.clone());
    identity.health_check.tls = Some(client_tls);
    f.config.upstreams.insert("identity".into(), identity);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
    let mut old = connect_named(f.address(), "localhost", &first, true, &[]).await?;
    roundtrip(&mut old, b"first").await?;
    origin.files.lock().unwrap().insert(
        "/cert.pem".into(),
        Bytes::from(std::fs::read(&second.certificate)?),
    );
    origin.files.lock().unwrap().insert(
        "/key.pem".into(),
        Bytes::from(std::fs::read(&second.private_key)?),
    );
    origin.files.lock().unwrap().insert(
        "/gated-cert.pem".into(),
        Bytes::from(std::fs::read(&second.certificate)?),
    );
    let mut still_first = connect_named(f.address(), "localhost", &first, true, &[]).await?;
    roundtrip(&mut still_first, b"no polling").await?;
    drop(still_first);
    assert_eq!(origin.calls.load(Ordering::Relaxed), 2);
    let mut candidate = p.snapshot().config.clone();
    let tls = candidate.listeners[0].tls.as_mut().unwrap();
    tls.certificates.clear();
    tls.certificate = format!("{url}/gated-cert.pem").into();
    tls.private_key = format!("{url}/key.pem").into();
    let applying = tokio::spawn({
        let p = p.clone();
        let text = candidate.encode()?;
        async move { p.apply(text, 0).await }
    });
    timeout(Duration::from_secs(5), origin.entered.notified()).await?;
    roundtrip(&mut old, b"traffic while fetching").await?;
    assert_eq!(p.snapshot().config.generation, 0);
    origin.release.notify_one();
    applying.await??;
    let mut new = connect_named(f.address(), "localhost", &second, true, &[]).await?;
    roundtrip(&mut new, b"second").await?;
    drop(new);
    roundtrip(&mut old, b"old session").await?;
    let durable = std::fs::read(&f.path)?;
    for path in ["/missing?token=private-token", "/bad"] {
        let mut bad = p.snapshot().config.clone();
        bad.listeners[0].tls.as_mut().unwrap().certificate = format!("{url}{path}").into();
        assert!(p.validate(bad.encode()?).await.is_err());
        let error = p.apply(bad.encode()?, 1).await.unwrap_err();
        assert!(!format!("{error:#}").contains("private-token"));
        assert_eq!(p.snapshot().config.generation, 1);
        assert_eq!(std::fs::read(&f.path)?, durable);
        roundtrip(&mut old, b"rollback").await?;
    }
    let connector = named_connector(&second, true, &[])?;
    origin.server.task.abort();
    drop(old);
    p.shutdown().await;
    drop(p);
    for tls in [&first, &second] {
        std::fs::remove_file(&tls.certificate)?;
        std::fs::remove_file(&tls.private_key)?;
    }
    let p = f.start().await;
    assert_eq!(p.snapshot().config.generation, 1);
    let mut io = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(f.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"offline restart").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}
