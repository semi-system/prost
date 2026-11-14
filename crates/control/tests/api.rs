use anyhow::Result;
use prost_control::{
    read_frame, request, write_frame, Command, ControlServer, Request, Response, MAX_FRAME,
};
use prost_core::ProxyBuilder;
use std::{os::unix::fs::PermissionsExt, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    time::timeout,
};

#[tokio::test]
async fn access_log_api_requires_generation_and_rotates_without_config_commit() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let log = dir.path().join("access.jsonl");
    let base = format!("schema_version=1\nlisteners=[]\n[upstreams]\n[runtime]\nadmin_socket={socket:?}\nartifact_dir={artifacts:?}\ndrain_seconds=1\n", artifacts=dir.path().join("artifacts"));
    std::fs::write(&path, &base)?;
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(proxy.clone()).await?;
    assert!(request(
        &socket,
        Command::FlushAccessLog {
            expected_generation: 0
        }
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("disabled"));
    server.shutdown().await;
    proxy.shutdown().await;
    drop(proxy);
    std::fs::write(&path, format!("{base}[runtime.access_log]\npath={log:?}\n"))?;
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(proxy.clone()).await?;
    let persisted = std::fs::read(&path)?;
    let rotated = dir.path().join("old.jsonl");
    std::fs::rename(&log, &rotated)?;
    let conflict = request(
        &socket,
        Command::ReopenAccessLog {
            expected_generation: 9,
        },
    )
    .await
    .unwrap_err();
    assert!(conflict.to_string().contains("generation_conflict"));
    assert!(!log.exists());
    let response = request(
        &socket,
        Command::ReopenAccessLog {
            expected_generation: 0,
        },
    )
    .await?;
    assert_eq!(response.generation, 0);
    assert_eq!(response.result.unwrap()["epoch"], 1);
    assert!(log.is_file());
    request(
        &socket,
        Command::FlushAccessLog {
            expected_generation: 0,
        },
    )
    .await?;
    assert_eq!(std::fs::read(&path)?, persisted);
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &serde_json::json!({"version":1,"id":"missing-generation","method":"access_log.reopen"}),
    )
    .await?;
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(2), io.read(&mut byte)).await??,
        0
    );
    server.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn socket_api_apply_validate_conflict_export_and_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let text=format!("schema_version=1\nlisteners=[]\n[upstreams]\n[runtime]\nadmin_socket={:?}\nartifact_dir={:?}\ndrain_seconds=1\n",socket.to_str().unwrap(),dir.path().join("artifacts").to_str().unwrap());
    std::fs::write(&path, &text)?;
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(proxy.clone()).await?;
    assert_eq!(
        std::fs::metadata(&socket)?.permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(request(&socket, Command::Status).await?.generation, 0);
    request(
        &socket,
        Command::Validate {
            config: text.clone(),
        },
    )
    .await?;
    assert_eq!(proxy.snapshot().config.generation, 0);
    let applied = request(
        &socket,
        Command::Apply {
            config: text.clone(),
            expected_generation: 0,
        },
    )
    .await?;
    assert_eq!(applied.generation, 1);
    let error = request(
        &socket,
        Command::Apply {
            config: text.clone(),
            expected_generation: 0,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("generation_conflict"));
    let exported = request(&socket, Command::Export).await?;
    assert!(exported.result.unwrap()["config"]
        .as_str()
        .unwrap()
        .contains("generation = 1"));
    let mut io = UnixStream::connect(&socket).await?;
    let req = Request {
        version: 99,
        id: "bad-version".into(),
        command: Command::Status,
    };
    write_frame(&mut io, &req).await?;
    let response: Response = read_frame(&mut io).await?;
    assert_eq!(response.id, "bad-version");
    assert_eq!(response.error.unwrap().code, "unsupported_api_version");
    let mut io = UnixStream::connect(&socket).await?;
    io.write_u32(MAX_FRAME as u32 + 1).await?;
    let mut b = [0];
    assert_eq!(timeout(Duration::from_secs(2), io.read(&mut b)).await??, 0);
    server.shutdown().await;
    proxy.shutdown().await;
    drop(proxy);
    assert!(!socket.exists());
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    assert_eq!(proxy.snapshot().config.generation, 1);
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn apply_survives_client_disconnect() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let text=format!("schema_version=1\nlisteners=[]\n[upstreams]\n[runtime]\nadmin_socket={:?}\nartifact_dir={:?}\ndrain_seconds=1\n",socket.to_str().unwrap(),dir.path().join("artifacts").to_str().unwrap());
    std::fs::write(&path, &text)?;
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(proxy.clone()).await?;
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &Request {
            version: 1,
            id: "disconnect".into(),
            command: Command::Apply {
                config: text,
                expected_generation: 0,
            },
        },
    )
    .await?;
    drop(io);
    timeout(Duration::from_secs(3), async {
        while proxy.snapshot().config.generation != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    server.shutdown().await;
    proxy.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn artifact_gc_wire_defaults_generation_limits_and_disconnect() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let store = dir.path().join("artifacts");
    let text = format!("schema_version=1\nlisteners=[]\n[upstreams]\n[runtime]\nworker_local=false\nadmin_socket={:?}\nartifact_dir={:?}\ndrain_seconds=1\n", socket.to_str().unwrap(), store.to_str().unwrap());
    std::fs::write(&path, text)?;
    let p = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(p.clone()).await?;
    let first = store.join("a".repeat(64));
    let second = store.join("b".repeat(64));
    std::fs::write(&first, b"orphan")?;
    std::fs::write(&second, b"orphan")?;
    let default: Command = serde_json::from_value(
        serde_json::json!({"method":"artifacts.gc","expected_generation":0}),
    )?;
    let response = request(&socket, default).await?;
    assert_eq!(response.generation, 0);
    let result = response.result.unwrap();
    assert_eq!(result["dry_run"], true);
    assert_eq!(result["deleted_files"], 0);
    assert_eq!(
        result["eligible_files"], 0,
        "age default must protect fresh artifacts"
    );
    let collect = |generation| Command::CollectArtifacts {
        dry_run: false,
        min_age_seconds: 0,
        max_entries: 1000,
        max_delete_files: 1,
        expected_generation: generation,
    };
    assert!(request(&socket, collect(99))
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_conflict"));
    let result = request(&socket, collect(0)).await?.result.unwrap();
    assert_eq!(result["deleted_files"], 1);
    assert_eq!(result["delete_limited"], true);
    assert_eq!(
        usize::from(first.exists()) + usize::from(second.exists()),
        1
    );
    let mut client = UnixStream::connect(&socket).await?;
    write_frame(
        &mut client,
        &Request {
            version: 1,
            id: "gc-disconnect".into(),
            command: collect(0),
        },
    )
    .await?;
    drop(client);
    timeout(Duration::from_secs(3), async {
        while first.exists() || second.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(p.snapshot().config.generation, 0);
    assert_eq!(p.status()["artifact_gc"]["deleted_files"], 1);
    server.shutdown().await;
    p.shutdown().await;
    Ok(())
}
