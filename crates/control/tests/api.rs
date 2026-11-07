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
