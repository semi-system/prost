use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_refresh_preserves_sessions_conflicts_rollback_gc_and_offline_restart() -> Result<()> {
    for mode in [
        prost_config::ReloadMode::Hot,
        prost_config::ReloadMode::Rolling,
    ] {
        let backend = echo().await;
        let mut f = Fixture::new(Protocol::Tcp, backend.address);
        f.config.runtime.reload_mode = mode;
        f.config.upstreams.get_mut("main").unwrap().idle_timeout_ms = 10000;
        let a = certificate(&f.dir.path().join("a"))?;
        let b = certificate_for(
            &f.dir.path().join("b"),
            &["localhost", "tenant.test", "unknown.test"],
        )?;
        f.config.listeners[0].tls = Some(a.clone());
        let mut plain = f.config.listeners[0].clone();
        plain.id = "plain".into();
        plain.address = free_address();
        plain.tls = None;
        let plain_address = plain.address;
        f.config.listeners.push(plain);
        std::fs::write(&f.path, f.config.encode()?)?;
        let p = f.start().await;
        let old = Arc::downgrade(&p.snapshot());
        let old_artifact = p.snapshot().config.listeners[0]
            .tls
            .as_ref()
            .unwrap()
            .certificate
            .clone();
        let mut held_tls = connect_named(f.address(), "localhost", &a, true, &[]).await?;
        let mut held_tcp = TcpStream::connect(plain_address).await?;
        roundtrip(&mut held_tls, b"before").await?;
        roundtrip(&mut held_tcp, b"before").await?;
        let mut next = b.clone();
        next.reject_unknown_sni = true;
        next.certificates.push(prost_config::NamedCertificate {
            server_names: vec!["localhost".into(), "tenant.test".into()],
            certificate: b.certificate.clone(),
            private_key: b.private_key.clone(),
        });
        let connector = named_connector(&b, true, &[])?;
        let (one, two) = tokio::join!(
            p.reload_tls("main".into(), next.clone(), 0),
            p.reload_tls("main".into(), next.clone(), 0)
        );
        let (success, conflict) = match (one, two) {
            (Ok(success), Err(conflict)) | (Err(conflict), Ok(success)) => (success, conflict),
            _ => anyhow::bail!("exactly one concurrent TLS update must commit"),
        };
        assert_eq!(success["generation"], 1);
        assert_eq!(success["worker_drain_started"], false);
        assert!(conflict.to_string().contains("generation_conflict"));
        assert_eq!(p.status()["worker_generations"]["retired_total"], 0);
        assert_eq!(p.status()["worker_generations"]["forced_total"], 0);
        let mut fresh = connect_named(f.address(), "tenant.test", &b, true, &[]).await?;
        roundtrip(&mut fresh, b"new certificate").await?;
        assert!(connect_named(f.address(), "localhost", &a, true, &[])
            .await
            .is_err());
        assert!(connect_named(f.address(), "unknown.test", &b, true, &[])
            .await
            .is_err());
        assert!(connect_named(f.address(), "localhost", &b, false, &[])
            .await
            .is_err());
        tokio::time::sleep(Duration::from_millis(1150)).await;
        roundtrip(&mut held_tls, b"past drain deadline").await?;
        roundtrip(&mut held_tcp, b"past drain deadline").await?;
        let disk = std::fs::read(&f.path)?;
        let mut bad = next.clone();
        bad.private_key = a.private_key.clone();
        assert!(p.reload_tls("main".into(), bad, 1).await.is_err());
        assert_eq!(p.snapshot().config.generation, 1);
        assert_eq!(std::fs::read(&f.path)?, disk);
        let backup = f.dir.path().join("saved.toml");
        std::fs::rename(&f.path, &backup)?;
        std::fs::create_dir(&f.path)?;
        assert!(p.reload_tls("main".into(), a.clone(), 1).await.is_err());
        std::fs::remove_dir(&f.path)?;
        std::fs::rename(&backup, &f.path)?;
        assert_eq!(p.snapshot().config.generation, 1);
        roundtrip(&mut held_tls, b"rollback keeps session").await?;
        p.collect_artifacts(
            prost_core::GcOptions {
                dry_run: false,
                min_age_seconds: 0,
                ..Default::default()
            },
            1,
        )
        .await?;
        assert!(
            old_artifact.exists(),
            "active old session must protect its artifacts"
        );
        drop((held_tls, held_tcp, fresh));
        timeout(Duration::from_secs(3), async {
            while old.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        p.collect_artifacts(
            prost_core::GcOptions {
                dry_run: false,
                min_age_seconds: 0,
                ..Default::default()
            },
            1,
        )
        .await?;
        assert!(!old_artifact.exists());
        // A later full config apply must still roll and retire the same workers.
        p.apply(p.snapshot().config.encode()?, 1).await?;
        p.wait_for_workers(2, 3000).await?;
        assert_eq!(
            p.status()["worker_generations"]["retired_total"],
            if mode == prost_config::ReloadMode::Rolling {
                1
            } else {
                0
            }
        );
        p.shutdown().await;
        drop(p);
        std::fs::remove_dir_all(f.dir.path().join("a"))?;
        std::fs::remove_dir_all(f.dir.path().join("b"))?;
        let p = f.start().await;
        assert_eq!(p.snapshot().config.generation, 2);
        let mut restored = connector
            .connect(
                "tenant.test".try_into()?,
                TcpStream::connect(f.address()).await?,
            )
            .await?;
        roundtrip(&mut restored, b"offline restart").await?;
        drop(restored);
        p.shutdown().await;
    }
    Ok(())
}
