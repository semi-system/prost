use super::*;

fn config(dir: &Path) -> AccessLog {
    AccessLog {
        path: dir.join("access.jsonl"),
        flush_interval_ms: 60000,
        ..Default::default()
    }
}
fn ticket(log: &Arc<Logger>, path: &str) -> Arc<Ticket> {
    Ticket::http(
        log.clone(),
        "127.0.0.1:1234",
        "http",
        7,
        "GET",
        "HTTP/2.0".into(),
        path,
    )
}
fn lines(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn completion_sampling_rotation_and_flush() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut c = config(dir.path());
    c.sample_every = 2;
    c.max_path_bytes = 8;
    let managed = dir.path().join("prost.toml");
    std::fs::write(&managed, "unchanged")?;
    let log = Logger::start(&c, &managed, &dir.path().join("artifacts"))?;
    assert!(log.sample());
    assert!(!log.sample());
    assert!(log.sample());
    let t = ticket(&log, "/abcdefghi");
    t.headers(200);
    t.finish(Some(3), "complete");
    t.finish(Some(99), "error");
    drop(t);
    let t = ticket(&log, "/ws");
    t.arm_tunnel();
    t.finish(Some(12), "complete");
    t.headers(101);
    drop(t);
    assert_eq!(log.control(false).await?, 0);
    let records = lines(&c.path);
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["path"], "/abcdefg");
    assert_eq!(records[0]["truncated"], true);
    assert_eq!(records[0]["body_bytes"], 3);
    assert_eq!(records[1]["protocol"], "websocket");
    assert_eq!(records[1]["status"], 101);
    let rotated = dir.path().join("rotated.jsonl");
    std::fs::rename(&c.path, &rotated)?;
    std::os::unix::fs::symlink(&managed, &c.path)?;
    assert!(log.control(true).await.is_err());
    assert_eq!(std::fs::read_to_string(&managed)?, "unchanged");
    std::fs::remove_file(&c.path)?;
    assert_eq!(log.control(true).await?, 1);
    let t = ticket(&log, "/new");
    t.headers(503);
    t.finish(Some(0), "complete");
    drop(t);
    log.shutdown().await?;
    assert_eq!(lines(&c.path).len(), 1);
    assert_eq!(lines(&rotated).len(), 2);
    assert_eq!(log.counters.active.load(Ordering::Relaxed), 0);
    assert_eq!(log.counters.sampled_out.load(Ordering::Relaxed), 1);
    Ok(())
}

#[test]
fn full_queue_is_bounded_and_receiver_close_is_accounted() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let log = Arc::new(Logger {
        sender,
        config: AccessLog::default(),
        path: PathBuf::new(),
        managed_config: PathBuf::new(),
        artifacts: PathBuf::new(),
        counters: Arc::new(Counters::default()),
        closed: AtomicBool::new(false),
        sequence: AtomicU64::new(0),
    });
    for _ in 0..1000 {
        let t = ticket(&log, "/");
        t.headers(200);
        t.finish(Some(1), "complete");
    }
    assert_eq!(log.counters.accepted.load(Ordering::Relaxed), 1);
    assert_eq!(log.counters.pending.load(Ordering::Relaxed), 1);
    assert_eq!(log.counters.dropped[0].load(Ordering::Relaxed), 999);
    drop(receiver);
    assert_eq!(log.counters.pending.load(Ordering::Relaxed), 0);
    assert_eq!(log.counters.dropped[2].load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn io_failure_suspends_and_reopen_restores_writes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let c = config(dir.path());
    std::fs::write(&c.path, "")?;
    // The writer gets a real read-only regular FD: buffered writes fail on flush.
    let file = File::open(&c.path)?;
    let (sender, receiver) = mpsc::sync_channel(8);
    let counters = Arc::new(Counters::default());
    counters.alive.store(1, Ordering::Relaxed);
    let stats = counters.clone();
    let thread =
        std::thread::spawn(move || writer_loop(file, receiver, stats, Duration::from_secs(60)));
    let log = Arc::new(Logger {
        sender,
        config: c.clone(),
        path: c.path.clone(),
        managed_config: dir.path().join("prost.toml"),
        artifacts: dir.path().join("artifacts"),
        counters,
        closed: AtomicBool::new(false),
        sequence: AtomicU64::new(0),
    });
    let t = ticket(&log, "/lost");
    t.headers(200);
    t.finish(Some(1), "complete");
    drop(t);
    assert!(log.control(false).await.is_err());
    assert_eq!(log.counters.io_errors.load(Ordering::Relaxed), 1);
    assert_eq!(log.counters.dropped[1].load(Ordering::Relaxed), 1);
    assert_eq!(log.control(true).await?, 1);
    let t = ticket(&log, "/ok");
    t.headers(200);
    t.finish(Some(2), "complete");
    drop(t);
    log.shutdown().await?;
    thread.join().unwrap();
    assert_eq!(lines(&c.path)[0]["path"], "/ok");
    assert_eq!(log.counters.alive.load(Ordering::Acquire), 0);
    Ok(())
}

#[test]
fn rejects_managed_paths_and_nonregular_files() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let managed = dir.path().join("prost.toml");
    let artifacts = dir.path().join("artifacts");
    std::fs::create_dir(&artifacts)?;
    for path in [
        managed.clone(),
        managed.with_extension("toml.lock"),
        artifacts.join("log"),
    ] {
        let c = AccessLog {
            path,
            ..Default::default()
        };
        assert!(Logger::start(&c, &managed, &artifacts).is_err());
    }
    let c = config(dir.path());
    std::fs::write(&managed, "config")?;
    std::fs::hard_link(&managed, &c.path)?;
    assert!(Logger::start(&c, &managed, &artifacts).is_err());
    std::fs::remove_file(&c.path)?;
    std::os::unix::fs::symlink(&managed, &c.path)?;
    assert!(Logger::start(&c, &managed, &artifacts).is_err());
    Ok(())
}
