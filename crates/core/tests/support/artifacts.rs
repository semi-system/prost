use super::*;
use prost_core::GcOptions;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_keeps_stream_generations_collects_validation_orphans_and_restarts() -> Result<()> {
    let backend = http_server("data").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    let first = certificate(&f.dir.path().join("first"))?;
    let second = certificate(&f.dir.path().join("second"))?;
    let validated = certificate(&f.dir.path().join("validate"))?;
    let restart_connector = named_connector(&second, true, &[b"h2"])?;
    f.config.listeners[0].tls = Some(first.clone());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let old = Arc::downgrade(&p.snapshot());
    let old_files = {
        let s = p.snapshot();
        let t = s.config.listeners[0].tls.as_ref().unwrap();
        [t.certificate.clone(), t.private_key.clone()]
    };
    let io = connect_named(f.address(), "localhost", &first, true, &[b"h2"]).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let request = |path| {
        Request::builder()
            .uri(format!("https://localhost{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let mut held = client.send_request(request("/hold")).await?;
    assert_eq!(
        held.body_mut().frame().await.unwrap()?.into_data().unwrap(),
        "data"
    );
    let mut next = p.snapshot().config.clone();
    next.listeners[0].tls = Some(second.clone());
    p.apply(next.encode()?, 0).await?;
    assert!(old.upgrade().is_some());
    let mut candidate = p.snapshot().config.clone();
    candidate.listeners[0].tls = Some(validated.clone());
    p.validate(candidate.encode()?).await?;
    let opts = GcOptions {
        min_age_seconds: 0,
        ..GcOptions::default()
    };
    let report = p.collect_artifacts(opts, 1).await?;
    assert_eq!(report.referenced_files, 4);
    assert_eq!(report.eligible_files, 2);
    assert_eq!(report.deleted_files, 0);
    let report = p
        .collect_artifacts(
            GcOptions {
                dry_run: false,
                ..opts
            },
            1,
        )
        .await?;
    assert_eq!(report.deleted_files, 2);
    assert_eq!(report.errors, 0);
    assert!(old_files.iter().all(|file| file.exists()));
    assert_eq!(
        client
            .send_request(request("/"))
            .await?
            .into_body()
            .collect()
            .await?
            .to_bytes(),
        "data"
    );
    assert!(p.collect_artifacts(opts, 0).await.is_err());
    drop(held);
    timeout(Duration::from_secs(3), async {
        while old.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let report = p
        .collect_artifacts(
            GcOptions {
                dry_run: false,
                ..opts
            },
            1,
        )
        .await?;
    assert_eq!(report.deleted_files, 2);
    assert!(old_files.iter().all(|file| !file.exists()));
    assert_eq!(p.status()["artifact_gc"]["deleted_files"], 2);
    let bytes = std::fs::read(&f.path)?;
    let stray = f.config.runtime.artifact_dir.join("0".repeat(64));
    std::fs::write(&stray, b"not referenced")?;
    std::fs::write(&f.path, b"externally changed")?;
    assert!(p
        .collect_artifacts(
            GcOptions {
                dry_run: false,
                ..opts
            },
            1
        )
        .await
        .is_err());
    assert!(stray.exists());
    std::fs::write(&f.path, bytes)?;
    for cert in [&first, &second, &validated] {
        std::fs::remove_file(&cert.certificate)?;
        std::fs::remove_file(&cert.private_key)?;
    }
    drop(client);
    driver.abort();
    p.shutdown().await;
    drop(p);
    let p = f.start().await;
    let io = restart_connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(f.address()).await?,
        )
        .await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    assert_eq!(client.send_request(request("/")).await?.status(), 200);
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
