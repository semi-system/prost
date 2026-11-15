use super::*;
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_rolling_preserves_old_tls_and_rejects_third_generation_without_blocking_certificates(
) -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    f.config.runtime.drain_seconds = 0;
    f.config.upstreams.get_mut("main").unwrap().idle_timeout_ms = 0;
    let a = certificate(&f.dir.path().join("a"))?;
    let b = certificate(&f.dir.path().join("b"))?;
    f.config.listeners[0].tls = Some(a.clone());
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut held = connect_named(f.address(), "localhost", &a, true, &[]).await?;
    roundtrip(&mut held, b"before rolling").await?;
    p.apply(p.snapshot().config.encode()?, 0).await?;
    assert!(p.status()["worker_generations"]["retiring"]["drain_remaining_ms"].is_null());
    tokio::time::sleep(Duration::from_millis(1150)).await;
    roundtrip(&mut held, b"unlimited drain").await?;
    let error = timeout(
        Duration::from_secs(2),
        p.apply(p.snapshot().config.encode()?, 1),
    )
    .await?
    .unwrap_err();
    assert!(error.to_string().contains("rolling_generation_busy"));
    p.reload_tls("main".into(), b.clone(), 1).await?;
    let mut fresh = connect_named(f.address(), "localhost", &b, true, &[]).await?;
    roundtrip(&mut fresh, b"certificate transaction").await?;
    roundtrip(&mut held, b"old certificate session").await?;
    drop(held);
    p.wait_for_workers(2, 3000).await?;
    drop(fresh);
    p.apply(p.snapshot().config.encode()?, 2).await?;
    p.wait_for_workers(3, 3000).await?;
    assert_eq!(p.status()["worker_generations"]["forced_total"], 0);
    p.shutdown().await;
    Ok(())
}
