use super::*;
use std::collections::BTreeMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_batch_is_atomic_and_expiry_metrics_follow_committed_certificates() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    let a = certificate(&f.dir.path().join("first"))?;
    let b = certificate(&f.dir.path().join("second"))?;
    f.config.listeners[0].tls = Some(a.clone());
    let mut second = f.config.listeners[0].clone();
    second.id = "second".into();
    second.address = free_address();
    let address = second.address;
    f.config.listeners.push(second);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let disk = std::fs::read(&f.path)?;
    assert!(p.reload_tls_many(BTreeMap::new(), 0).await.is_err());
    let mut invalid = b.clone();
    invalid.private_key = a.private_key.clone();
    assert!(p
        .reload_tls_many(
            BTreeMap::from([("main".into(), b.clone()), ("second".into(), invalid)]),
            0
        )
        .await
        .is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    assert_eq!(std::fs::read(&f.path)?, disk);
    for endpoint in [f.address(), address] {
        let mut io = connect_named(endpoint, "localhost", &a, true, &[]).await?;
        roundtrip(&mut io, b"rollback").await?;
    }
    p.reload_tls_many(
        BTreeMap::from([("main".into(), b.clone()), ("second".into(), b.clone())]),
        0,
    )
    .await?;
    assert_eq!(p.snapshot().config.generation, 1);
    assert_eq!(p.status()["worker_generations"]["retired_total"], 0);
    let expiry = prost_transport::certificate_info(&std::fs::read(&b.certificate)?)?.not_after;
    let families = p.metrics.registry.gather();
    let family = families
        .iter()
        .find(|family| family.get_name() == "prost_tls_certificate_not_after_seconds")
        .context("missing certificate expiry metrics")?;
    assert_eq!(family.get_metric().len(), 2);
    assert!(family
        .get_metric()
        .iter()
        .all(|metric| metric.get_gauge().get_value() == expiry as f64));
    for endpoint in [f.address(), address] {
        let mut io = connect_named(endpoint, "localhost", &b, true, &[]).await?;
        roundtrip(&mut io, b"atomic commit").await?;
    }
    p.shutdown().await;
    Ok(())
}
