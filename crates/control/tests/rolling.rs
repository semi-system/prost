use anyhow::{Context, Result};
use prost_control::{request, write_frame, Command, ControlServer, Request};
use prost_core::ProxyBuilder;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    task::JoinSet,
    time::timeout,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_api_bounds_waiters_preserves_admin_capacity_and_cancels_on_shutdown() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_address = backend.local_addr()?;
    let echo = tokio::spawn(async move {
        while let Ok((mut io, _)) = backend.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = io.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    let reserved = TcpListener::bind("127.0.0.1:0").await?;
    let address = reserved.local_addr()?;
    drop(reserved);
    let text = format!("schema_version=1\n[runtime]\nadmin_socket={socket:?}\nartifact_dir={artifacts:?}\nio_workers=1\ndrain_seconds=30\n[[upstreams.echo.endpoints]]\nid='one'\naddress='{backend_address}'\n[[listeners]]\nid='tcp'\naddress='{address}'\nprotocol='tcp'\nupstream='echo'\n", artifacts=dir.path().join("artifacts"));
    std::fs::write(&path, &text)?;
    let proxy = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(proxy.clone()).await?;
    let mut old = TcpStream::connect(address).await?;
    old.write_all(b"a").await?;
    assert_eq!(old.read_u8().await?, b'a');
    request(
        &socket,
        Command::Apply {
            config: text,
            expected_generation: 0,
        },
    )
    .await?;
    let persisted = std::fs::read(&path)?;
    let error = request(
        &socket,
        Command::WaitWorkers {
            generation: 1,
            timeout_ms: 10,
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("worker_drain_timeout"));
    assert_eq!(proxy.snapshot().config.generation, 1);
    assert_eq!(std::fs::read(&path)?, persisted);
    assert!(request(
        &socket,
        Command::WaitWorkers {
            generation: 2,
            timeout_ms: 100
        }
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("generation_not_committed"));
    let mut waits = JoinSet::new();
    for _ in 0..9 {
        let socket = socket.clone();
        waits.spawn(async move {
            request(
                &socket,
                Command::WaitWorkers {
                    generation: 1,
                    timeout_ms: 5000,
                },
            )
            .await
        });
    }
    let first = timeout(Duration::from_secs(2), waits.join_next())
        .await?
        .context("missing waiter")??;
    assert!(first.unwrap_err().to_string().contains("busy"));
    assert_eq!(request(&socket, Command::Status).await?.generation, 1);
    request(
        &socket,
        Command::Validate {
            config: proxy.snapshot().config.encode()?,
        },
    )
    .await?;
    old.write_all(b"b").await?;
    assert_eq!(old.read_u8().await?, b'b');
    drop(old);
    while let Some(result) = timeout(Duration::from_secs(2), waits.join_next()).await? {
        assert_eq!(result??.result.context("missing result")?["drained"], true);
    }
    let mut held = TcpStream::connect(address).await?;
    held.write_all(b"c").await?;
    assert_eq!(held.read_u8().await?, b'c');
    proxy.apply(proxy.snapshot().config.encode()?, 1).await?;
    let mut wait_socket = UnixStream::connect(&socket).await?;
    write_frame(
        &mut wait_socket,
        &Request {
            version: 1,
            id: "shutdown-wait".into(),
            command: Command::WaitWorkers {
                generation: 2,
                timeout_ms: 300000,
            },
        },
    )
    .await?;
    timeout(Duration::from_secs(1), server.shutdown()).await?;
    let closed = timeout(Duration::from_secs(1), wait_socket.read_u8())
        .await?
        .unwrap_err();
    // Shutdown may win before the request frame is consumed. Linux reports
    // reset for unread data, or EOF after dispatch; both mean the wait closed.
    assert!(matches!(
        closed.kind(),
        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
    ));
    held.write_all(b"d").await?;
    assert_eq!(held.read_u8().await?, b'd');
    drop(held);
    timeout(Duration::from_secs(3), proxy.shutdown()).await?;
    echo.abort();
    Ok(())
}
