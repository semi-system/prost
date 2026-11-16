use super::*;

async fn fetch(
    address: SocketAddr,
    h2: bool,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<Response<Bytes>> {
    let mut request = Request::builder()
        .method(method)
        .uri(format!("http://localhost{path}"));
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let request = request.body(Full::new(Bytes::new()))?;
    let io = TokioIo::new(TcpStream::connect(address).await?);
    let response = if h2 {
        let (mut client, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), io).await?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        client.ready().await?;
        let response = client.send_request(request).await?;
        let (parts, body) = response.into_parts();
        let data = timeout(Duration::from_secs(5), body.collect())
            .await??
            .to_bytes();
        driver.abort();
        Response::from_parts(parts, data)
    } else {
        let (mut client, connection) = hyper::client::conn::http1::handshake(io).await?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let response = client.send_request(request).await?;
        let (parts, body) = response.into_parts();
        let data = timeout(Duration::from_secs(5), body.collect())
            .await??
            .to_bytes();
        driver.abort();
        Response::from_parts(parts, data)
    };
    Ok(response)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_h1_h2_indexes_ranges_conditions_and_confined_paths() -> Result<()> {
    let mut f = Fixture::new(Protocol::Http, free_address());
    let root = f.dir.path().join("www");
    std::fs::create_dir_all(root.join("docs"))?;
    std::fs::write(root.join("docs/index.html"), b"<h1>static</h1>")?;
    std::fs::write(root.join("data.bin"), b"0123456789")?;
    std::fs::write(f.dir.path().join("private.key"), b"must-not-leak")?;
    std::os::unix::fs::symlink(f.dir.path().join("private.key"), root.join("link"))?;
    std::os::unix::fs::symlink(f.dir.path(), root.join("escape"))?;
    f.config.listeners[0].upstream = None;
    f.config.upstreams.clear();
    f.config.listeners[0].routes = vec![serde_json::from_value(
        serde_json::json!({"path_prefix":"/public/","static_files":{"root":root,"strip_prefix":true}}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    for h2 in [false, true] {
        let r = fetch(f.address(), h2, "GET", "/public/docs?a=1", &[]).await?;
        assert_eq!(r.status(), 301);
        assert_eq!(r.headers()["location"], "/public/docs/?a=1");
        let r = fetch(f.address(), h2, "GET", "/public/docs/", &[]).await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["content-type"], "text/html; charset=utf-8");
        assert_eq!(r.body(), "<h1>static</h1>");
        let r = fetch(f.address(), h2, "GET", "/public/data.bin", &[]).await?;
        assert_eq!(r.body(), "0123456789");
        let tag = r.headers()["etag"].to_str()?;
        let modified = r.headers()["last-modified"].to_str()?;
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("if-none-match", tag)]
            )
            .await?
            .status(),
            304
        );
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("if-modified-since", modified)]
            )
            .await?
            .status(),
            304
        );
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[
                    ("if-none-match", "\"other\""),
                    ("if-modified-since", modified)
                ]
            )
            .await?
            .status(),
            200
        );
        for (range, expected) in [
            ("bytes=2-5", "2345"),
            ("bytes=-3", "789"),
            ("bytes=8-", "89"),
        ] {
            let r = fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("range", range)],
            )
            .await?;
            assert_eq!(r.status(), 206);
            assert_eq!(r.body(), expected);
        }
        let r = fetch(
            f.address(),
            h2,
            "GET",
            "/public/data.bin",
            &[("range", "bytes=100-")],
        )
        .await?;
        assert_eq!(r.status(), 416);
        assert_eq!(r.headers()["content-range"], "bytes */10");
        let r = fetch(
            f.address(),
            h2,
            "GET",
            "/public/data.bin",
            &[("range", "bytes=2-3"), ("if-range", "\"stale\"")],
        )
        .await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.body(), "0123456789");
        let r = fetch(f.address(), h2, "HEAD", "/public/data.bin", &[]).await?;
        assert_eq!(r.headers()["content-length"], "10");
        assert!(r.body().is_empty());
        let r = fetch(
            f.address(),
            h2,
            "HEAD",
            "/public/data.bin",
            &[("range", "bytes=0-1")],
        )
        .await?;
        assert_eq!(r.status(), 200);
        assert_eq!(r.headers()["content-length"], "10");
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("if-match", "*")]
            )
            .await?
            .status(),
            200
        );
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("if-match", tag)]
            )
            .await?
            .status(),
            412
        );
        assert_eq!(
            fetch(
                f.address(),
                h2,
                "GET",
                "/public/data.bin",
                &[("if-unmodified-since", "Thu, 01 Jan 1970 00:00:00 GMT")]
            )
            .await?
            .status(),
            412
        );
        assert_eq!(
            fetch(f.address(), h2, "POST", "/public/data.bin", &[])
                .await?
                .status(),
            405
        );
        assert_eq!(
            fetch(f.address(), h2, "GET", "/public/missing", &[])
                .await?
                .status(),
            404
        );
        assert_eq!(
            fetch(f.address(), h2, "GET", "/public/link", &[])
                .await?
                .status(),
            403
        );
        for path in [
            "/public/%2e%2e/private.key",
            "/public/%2fprivate.key",
            "/public/escape/private.key",
        ] {
            let r = fetch(f.address(), h2, "GET", path, &[]).await?;
            assert!([403, 404].contains(&r.status().as_u16()), "{path}");
            assert!(!r.body().windows(13).any(|w| w == b"must-not-leak"));
        }
    }
    let mut bad = p.snapshot().config.clone();
    bad.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .root = root.join("missing");
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    assert_eq!(
        fetch(f.address(), true, "GET", "/public/data.bin", &[])
            .await?
            .body(),
        "0123456789"
    );
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_ram_snapshot_refresh_and_failed_validation_preserve_live_image() -> Result<()> {
    let mut f = Fixture::new(Protocol::Http, free_address());
    let root = f.dir.path().join("ram");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("index.html"), b"ram-v1")?;
    std::fs::write(root.join("data.bin"), vec![b'x'; 128 * 1024])?;
    f.config.runtime.max_static_ram_bytes = 300_000;
    f.config.listeners[0].upstream = None;
    f.config.upstreams.clear();
    f.config.listeners[0].routes = vec![serde_json::from_value(
        serde_json::json!({"static_files":{"root":root,"load_to_ram":true,"max_ram_bytes":200_000}}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let reserved = p.status()["static_files_ram"]["reserved_bytes"]
        .as_u64()
        .unwrap();
    assert!(reserved > 128 * 1024 && reserved < 200_000);
    // Requests use only the prepared image: changing/removing the source does
    // not change a response and never falls back to disk for a new file.
    std::fs::remove_file(root.join("index.html"))?;
    std::fs::write(root.join("new.txt"), b"not-loaded")?;
    for h2 in [false, true] {
        assert_eq!(
            fetch(f.address(), h2, "GET", "/", &[]).await?.body(),
            "ram-v1"
        );
        assert_eq!(
            fetch(f.address(), h2, "GET", "/new.txt", &[])
                .await?
                .status(),
            404
        );
        let r = fetch(
            f.address(),
            h2,
            "GET",
            "/data.bin",
            &[("range", "bytes=10-19")],
        )
        .await?;
        assert_eq!(r.status(), 206);
        assert_eq!(r.body(), "xxxxxxxxxx");
    }
    std::fs::write(root.join("index.html"), b"ram-v2")?;
    let mut candidate = p.snapshot().config.clone();
    // Sparse file: reject by metadata before trying to allocate or read 8GiB.
    let huge = std::fs::File::create(root.join("huge.bin"))?;
    huge.set_len(8 * 1024 * 1024 * 1024)?;
    assert!(p.validate(candidate.encode()?).await.is_err());
    assert!(p.apply(candidate.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    assert_eq!(p.status()["static_files_ram"]["reserved_bytes"], reserved);
    assert_eq!(
        fetch(f.address(), true, "GET", "/", &[]).await?.body(),
        "ram-v1"
    );
    drop(huge);
    std::fs::remove_file(root.join("huge.bin"))?;
    // A route may fit alone while active + two candidate copies do not.
    let mut second = candidate.listeners[0].routes[0].clone();
    second.path_prefix = "/another/".into();
    candidate.listeners[0].routes.push(second);
    let error = p.validate(candidate.encode()?).await.unwrap_err();
    assert!(
        format!("{error:#}").contains("process budget exceeded"),
        "{error:#}"
    );
    assert_eq!(p.status()["static_files_ram"]["reserved_bytes"], reserved);
    candidate.listeners[0].routes.pop();
    candidate.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .max_ram_files = 1;
    assert!(p.validate(candidate.encode()?).await.is_err());
    candidate.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .max_ram_files = 10_000;
    candidate.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .ram_headroom_bytes = 1_u64 << 40;
    assert!(p.validate(candidate.encode()?).await.is_err());
    candidate.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .ram_headroom_bytes = 64 * 1024 * 1024;
    p.validate(candidate.encode()?).await?;
    assert_eq!(p.status()["static_files_ram"]["reserved_bytes"], reserved);
    p.apply(candidate.encode()?, 0).await?;
    for h2 in [false, true] {
        assert_eq!(
            fetch(f.address(), h2, "GET", "/", &[]).await?.body(),
            "ram-v2"
        );
        assert_eq!(
            fetch(f.address(), h2, "GET", "/new.txt", &[]).await?.body(),
            "not-loaded"
        );
    }
    for _ in 0..100 {
        if p.status()["static_files_ram"]["reserved_bytes"]
            .as_u64()
            .unwrap()
            < 200_000
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        p.status()["static_files_ram"]["reserved_bytes"]
            .as_u64()
            .unwrap()
            < 200_000
    );
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn static_disk_demand_cache_revalidates_evicts_and_bypasses_memory_pressure() -> Result<()> {
    let mut f = Fixture::new(Protocol::Http, free_address());
    let root = f.dir.path().join("disk");
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("a.txt"), vec![b'a'; 1024])?;
    std::fs::write(root.join("b.txt"), vec![b'b'; 1024])?;
    f.config.listeners[0].upstream = None;
    f.config.upstreams.clear();
    f.config.listeners[0].routes = vec![serde_json::from_value(
        serde_json::json!({"static_files":{"root":root,"max_ram_bytes":3000,"cache_max_file_bytes":2048}}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    assert_eq!(p.status()["static_files"][0]["cache"]["entries"], 0);
    for h2 in [false, true] {
        assert_eq!(
            fetch(f.address(), h2, "GET", "/a.txt", &[])
                .await?
                .body()
                .as_ref(),
            vec![b'a'; 1024]
        );
    }
    assert_eq!(p.status()["static_files"][0]["cache"]["entries"], 1);
    assert!(
        p.status()["static_files"][0]["cache"]["hits"]
            .as_u64()
            .unwrap()
            > 0
    );
    // Same size and preserved mtime: ctime/inode validation still invalidates.
    let old_tag = fetch(f.address(), true, "GET", "/a.txt", &[])
        .await?
        .headers()["etag"]
        .to_str()?
        .to_owned();
    let modified = std::fs::metadata(root.join("a.txt"))?.modified()?;
    std::fs::write(root.join("a.txt"), vec![b'z'; 1024])?;
    std::fs::File::options()
        .write(true)
        .open(root.join("a.txt"))?
        .set_times(std::fs::FileTimes::new().set_modified(modified))?;
    assert_eq!(
        fetch(f.address(), true, "GET", "/a.txt", &[])
            .await?
            .body()
            .as_ref(),
        vec![b'z'; 1024]
    );
    assert_eq!(
        fetch(
            f.address(),
            true,
            "GET",
            "/a.txt",
            &[("if-none-match", &old_tag)]
        )
        .await?
        .status(),
        200
    );
    std::fs::rename(&root, f.dir.path().join("old-disk-tree"))?;
    std::fs::create_dir(&root)?;
    std::fs::write(root.join("a.txt"), vec![b'n'; 1024])?;
    std::fs::write(root.join("b.txt"), vec![b'b'; 1024])?;
    assert_eq!(
        fetch(f.address(), true, "GET", "/a.txt", &[])
            .await?
            .body()
            .as_ref(),
        vec![b'n'; 1024]
    );
    assert_eq!(
        fetch(f.address(), true, "GET", "/b.txt", &[])
            .await?
            .body()
            .as_ref(),
        vec![b'b'; 1024]
    );
    assert!(
        p.status()["static_files"][0]["cache"]["entries"]
            .as_u64()
            .unwrap()
            <= 1
    );
    assert!(
        p.status()["static_files"][0]["cache"]["reserved_bytes"]
            .as_u64()
            .unwrap()
            <= 3000
    );
    std::fs::remove_file(root.join("b.txt"))?;
    assert_eq!(
        fetch(f.address(), false, "GET", "/b.txt", &[])
            .await?
            .status(),
        404
    );
    // Unlike forced preload, a disk cache that cannot reserve RAM must serve
    // the file successfully, without silently accepting a partial RAM image.
    let mut candidate = p.snapshot().config.clone();
    candidate.listeners[0].routes[0]
        .static_files
        .as_mut()
        .unwrap()
        .ram_headroom_bytes = 1_u64 << 40;
    p.validate(candidate.encode()?).await?;
    p.apply(candidate.encode()?, 0).await?;
    assert_eq!(
        fetch(f.address(), true, "GET", "/a.txt", &[])
            .await?
            .body()
            .as_ref(),
        vec![b'n'; 1024]
    );
    assert_eq!(p.status()["static_files"][0]["cache"]["entries"], 0);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
#[ignore = "run with scripts/check-static-memory.sh inside a 96MiB cgroup"]
async fn static_ram_cgroup_limit_rejects_before_allocation() -> Result<()> {
    let limit = std::fs::read_to_string("/sys/fs/cgroup/memory.max")?
        .trim()
        .parse::<u64>()?;
    assert_eq!(
        limit,
        96 * 1024 * 1024,
        "this contract must run inside the bounded container"
    );
    let mut f = Fixture::new(Protocol::Http, free_address());
    let root = f.dir.path().join("bounded");
    std::fs::create_dir(&root)?;
    std::fs::File::create(root.join("sparse.bin"))?.set_len(96 * 1024 * 1024)?;
    f.config.listeners[0].upstream = None;
    f.config.upstreams.clear();
    f.config.listeners[0].routes = vec![serde_json::from_value(
        serde_json::json!({"static_files":{"root":root,"load_to_ram":true,"max_ram_bytes":134217728,"ram_headroom_bytes":16777216}}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let before = std::fs::read(&f.path)?;
    let error = match ProxyBuilder::new(&f.path)?.start().await {
        Ok(p) => {
            p.shutdown().await;
            anyhow::bail!("oversized RAM image was accepted")
        }
        Err(e) => e,
    };
    assert!(
        format!("{error:#}").contains("insufficient available RAM"),
        "{error:#}"
    );
    assert_eq!(std::fs::read(&f.path)?, before);
    let events = std::fs::read_to_string("/sys/fs/cgroup/memory.events")?;
    assert!(events.lines().any(|line| line == "oom_kill 0"), "{events}");
    Ok(())
}
