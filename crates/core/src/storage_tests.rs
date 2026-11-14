use super::*;
use storage::WritePhase;

fn fixture() -> Result<(tempfile::TempDir, PathBuf)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let text = format!("schema_version=1\nlisteners=[]\n[upstreams]\n[runtime]\nworker_local=false\ndrain_seconds=1\nadmin_socket={:?}\nartifact_dir={:?}\n", dir.path().join("admin.sock").to_str().unwrap(), dir.path().join("artifacts").to_str().unwrap());
    std::fs::write(&path, text)?;
    Ok((dir, path))
}
#[tokio::test]
async fn persistence_failures_before_rename_never_publish() -> Result<()> {
    for phase in [
        WritePhase::DirectoryBefore,
        WritePhase::Create,
        WritePhase::Write,
        WritePhase::FileSync,
        WritePhase::Rename,
    ] {
        let (_dir, path) = fixture()?;
        let p = ProxyBuilder::new(&path)?.start().await?;
        let before = std::fs::read(&path)?;
        let candidate = p.snapshot().config.encode()?;
        *p.artifacts.fault.lock().unwrap() = Some((phase, false));
        assert!(p.apply(candidate, 0).await.is_err(), "{phase:?}");
        assert_eq!(p.snapshot().config.generation, 0, "{phase:?}");
        assert_eq!(std::fs::read(&path)?, before, "{phase:?}");
        assert!(!std::fs::read_dir(path.parent().unwrap())?
            .filter_map(Result::ok)
            .any(|e| e.file_name().to_string_lossy().ends_with(".tmp")));
        p.shutdown().await;
        drop(p);
        let p = ProxyBuilder::new(&path)?.start().await?;
        assert_eq!(p.snapshot().config.generation, 0);
        p.shutdown().await;
    }
    Ok(())
}
#[tokio::test]
async fn post_rename_fsync_failure_publishes_and_can_repair_before_gc() -> Result<()> {
    let (_dir, path) = fixture()?;
    let p = ProxyBuilder::new(&path)?.start().await?;
    *p.artifacts.fault.lock().unwrap() = Some((WritePhase::DirectoryAfter, false));
    let response = p.apply(p.snapshot().config.encode()?, 0).await?;
    assert_eq!(response["durable"], false);
    assert_eq!(p.snapshot().config.generation, 1);
    assert_eq!(
        Config::parse(&std::fs::read_to_string(&path)?)?.generation,
        1
    );
    let report = p
        .collect_artifacts(
            GcOptions {
                dry_run: false,
                min_age_seconds: 0,
                ..GcOptions::default()
            },
            1,
        )
        .await?;
    assert!(report.durable);
    p.shutdown().await;
    drop(p);
    let p = ProxyBuilder::new(&path)?.start().await?;
    assert_eq!(p.snapshot().config.generation, 1);
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_validate_keeps_gc_serialized_until_staging_ends() -> Result<()> {
    let (_dir, path) = fixture()?;
    let p = ProxyBuilder::new(&path)?.start().await?;
    let (entered, rx) = std::sync::mpsc::sync_channel(1);
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    *p.artifacts.stage_pause.lock().unwrap() = Some((entered, wait));
    let task = {
        let p = p.clone();
        let text = p.snapshot().config.encode()?;
        tokio::spawn(async move { p.validate(text).await })
    };
    tokio::task::spawn_blocking(move || rx.recv_timeout(Duration::from_secs(3))).await??;
    task.abort();
    let _ = task.await;
    let gc = {
        let p = p.clone();
        tokio::spawn(async move { p.collect_artifacts(GcOptions::default(), 0).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    let serialized = !gc.is_finished();
    release.send(())?;
    assert!(
        serialized,
        "GC entered while detached staging was still running"
    );
    tokio::time::timeout(Duration::from_secs(3), gc).await???;
    p.shutdown().await;
    Ok(())
}

#[test]
fn crash_boundaries_restart_with_authoritative_config() -> Result<()> {
    for (label, expected) in [
        ("before_rename", 0),
        ("after_rename", 1),
        ("after_fsync", 1),
    ] {
        let (_dir, path) = fixture()?;
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "storage_tests::crash_fixture",
                "--nocapture",
            ])
            .env("PROST_TEST_CRASH_PATH", &path)
            .env("PROST_TEST_CRASH_PHASE", label)
            .output()?;
        assert_eq!(
            output.status.code(),
            Some(73),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let rt = tokio::runtime::Runtime::new()?;
        rt.block_on(async {
            let p = ProxyBuilder::new(&path)?.start().await?;
            assert_eq!(p.snapshot().config.generation, expected, "{label}");
            p.shutdown().await;
            Ok::<_, anyhow::Error>(())
        })?;
    }
    Ok(())
}
#[test]
#[ignore = "subprocess fixture used by crash_boundaries_restart_with_authoritative_config"]
fn crash_fixture() -> Result<()> {
    let path = PathBuf::from(std::env::var_os("PROST_TEST_CRASH_PATH").context("test path")?);
    let phase = match std::env::var("PROST_TEST_CRASH_PHASE")?.as_str() {
        "before_rename" => WritePhase::Rename,
        "after_rename" => WritePhase::DirectoryAfter,
        "after_fsync" => WritePhase::Complete,
        _ => anyhow::bail!("invalid test phase"),
    };
    tokio::runtime::Runtime::new()?.block_on(async {
        let p = ProxyBuilder::new(&path)?.start().await?;
        *p.artifacts.fault.lock().unwrap() = Some((phase, true));
        p.apply(p.snapshot().config.encode()?, 0).await?;
        anyhow::bail!("expected child to terminate at persistence boundary")
    })
}
