use anyhow::Result;
use prost_control::{
    read_frame, request, write_frame, Command, ControlServer, RateTableTarget, Request, TableKey,
    TableQuery,
};
use prost_core::ProxyBuilder;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    time::timeout,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_rate_table_api_conflicts_disconnect_and_explicit_clear() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let backend = TcpListener::bind("127.0.0.1:0").await?;
    let backend_address = backend.local_addr()?;
    let backend_task = tokio::spawn(async move {
        while let Ok((mut io, _)) = backend.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = io.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    let reservation = TcpListener::bind("127.0.0.1:0").await?;
    let address = reservation.local_addr()?;
    drop(reservation);
    let config = format!(
        r#"schema_version=1
[runtime]
admin_socket={socket:?}
artifact_dir={artifacts:?}
drain_seconds=1
observe_plugins=true
[upstreams.main]
health_interval_ms=60000
[[upstreams.main.endpoints]]
id="one"
address={backend:?}
[[listeners]]
id="tcp"
address={address:?}
protocol="tcp"
upstream="main"
[[listeners.plugins]]
name="connection_window"
hooks=["connection"]
config={{max_connections=1,window_ms=60000}}
"#,
        artifacts = dir.path().join("artifacts"),
        backend = backend_address.to_string(),
        address = address.to_string()
    );
    std::fs::write(&path, &config)?;
    let p = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(p.clone()).await?;
    let listed = request(
        &socket,
        Command::RateTableList {
            listener: None,
            limit: 100,
        },
    )
    .await?
    .result
    .unwrap();
    assert_eq!(listed["total_tables"], 1);
    assert_eq!(listed["tables"][0]["plugin_name"], "connection_window");
    assert!(request(
        &socket,
        Command::RateTableList {
            listener: Some("missing".into()),
            limit: 100
        }
    )
    .await
    .is_err());
    let target = || RateTableTarget {
        listener: "tcp".into(),
        route: None,
        plugin: 0,
    };
    let show = || Command::RateTableShow {
        target: target(),
        query: TableQuery::default(),
    };
    let clear = |expected| Command::RateTableClear {
        target: target(),
        key: Some(TableKey::text("::ffff:127.0.0.1")),
        all: false,
        expected_generation: expected,
    };
    let mut live = TcpStream::connect(address).await?;
    live.write_all(b"a").await?;
    assert_eq!(
        timeout(Duration::from_secs(2), live.read_u8()).await??,
        b'a'
    );
    let persisted = std::fs::read(&path)?;
    let shown = request(&socket, show()).await?;
    assert_eq!(shown.generation, 0);
    assert_eq!(
        shown.result.as_ref().unwrap()["table"]["retained_entries"],
        1
    );
    assert!(request(&socket, clear(9))
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_conflict"));
    assert_eq!(
        request(&socket, show()).await?.result.unwrap()["table"]["retained_entries"],
        1
    );
    for (key, all) in [
        (None, false),
        (Some(TableKey::text("127.0.0.1")), true),
        (Some(TableKey::text("invalid")), false),
    ] {
        assert!(request(
            &socket,
            Command::RateTableClear {
                target: target(),
                key,
                all,
                expected_generation: 0
            }
        )
        .await
        .is_err());
    }
    for limit in [0, 1025] {
        assert!(request(
            &socket,
            Command::RateTableShow {
                target: target(),
                query: TableQuery {
                    limit,
                    ..Default::default()
                }
            }
        )
        .await
        .is_err());
    }
    // A missing expected generation cannot default to an unconditional clear.
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(&mut io,&serde_json::json!({"version":1,"id":"no-generation","method":"rate_table.clear","target":target(),"all":true})).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), io.read(&mut [0])).await??,
        0
    );
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &Request {
            version: 1,
            id: "clear-disconnect".into(),
            command: clear(0),
        },
    )
    .await?;
    drop(io);
    timeout(Duration::from_secs(3), async {
        loop {
            if request(&socket, show()).await.unwrap().result.unwrap()["table"]["retained_entries"]
                == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(std::fs::read(&path)?, persisted);
    live.write_all(b"b").await?;
    assert_eq!(
        timeout(Duration::from_secs(2), live.read_u8()).await??,
        b'b'
    );
    let mut fresh = TcpStream::connect(address).await?;
    fresh.write_all(b"c").await?;
    assert_eq!(
        timeout(Duration::from_secs(2), fresh.read_u8()).await??,
        b'c'
    );
    let cleared = request(
        &socket,
        Command::RateTableClear {
            target: target(),
            key: None,
            all: true,
            expected_generation: 0,
        },
    )
    .await?;
    assert_eq!(cleared.generation, 0);
    assert_eq!(cleared.result.as_ref().unwrap()["affected_entries"], 1);
    assert_eq!(cleared.result.as_ref().unwrap()["ephemeral"], true);
    request(
        &socket,
        Command::Apply {
            config: config.clone(),
            expected_generation: 0,
        },
    )
    .await?;
    assert!(request(&socket, clear(0))
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_conflict"));
    // Reads report their pinned snapshot's generation in both envelope and result.
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &Request {
            version: 1,
            id: "read-generation".into(),
            command: show(),
        },
    )
    .await?;
    let read: prost_control::Response = read_frame(&mut io).await?;
    assert_eq!(read.generation, read.result.unwrap()["generation"]);
    drop(live);
    drop(fresh);
    server.shutdown().await;
    p.shutdown().await;
    backend_task.abort();
    Ok(())
}
