use anyhow::Result;
use prost_control::{
    read_frame, request, write_frame, CacheKeyInput, CacheQuery, Command, ControlServer, Request,
    Response,
};
use prost_core::ProxyBuilder;
use std::time::Duration;
use tokio::{io::AsyncReadExt, net::UnixStream, time::timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_api_scope_generation_disconnect_and_apply_serialization() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("prost.toml");
    let socket = dir.path().join("admin.sock");
    let config = format!(
        r#"schema_version=1
listeners=[]
[runtime]
admin_socket={socket:?}
artifact_dir={artifacts:?}
io_workers=2
drain_seconds=1
[upstreams.cached.http.cache]
max_bytes=65536
max_object_bytes=4096
[[upstreams.cached.endpoints]]
id="one"
address="127.0.0.1:9"
[[upstreams.disabled.endpoints]]
id="one"
address="127.0.0.1:9"
"#,
        artifacts = dir.path().join("artifacts")
    );
    std::fs::write(&path, &config)?;
    let p = ProxyBuilder::new(&path)?.start().await?;
    let server = ControlServer::bind(p.clone()).await?;
    let status = || Command::CacheStatus {
        upstream: Some("cached".into()),
    };
    let purge = |expected| Command::CachePurge {
        upstream: "cached".into(),
        key: None,
        all: true,
        expected_generation: expected,
    };
    let persisted = std::fs::read(&path)?;
    let all = request(&socket, Command::CacheStatus { upstream: None })
        .await?
        .result
        .unwrap();
    assert_eq!(all["caches"]["cached"]["enabled"], true);
    assert_eq!(all["caches"]["disabled"]["enabled"], false);
    let input = CacheKeyInput {
        uri: "/a?q=1".into(),
        host: Some("cache.test".into()),
        accept_encoding: vec!["gzip".into()],
    };
    let key = request(
        &socket,
        Command::CacheKey {
            input: input.clone(),
        },
    )
    .await?
    .result
    .unwrap()["key"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(key, prost_core::cache_key(&input)?);
    let entries = request(
        &socket,
        Command::CacheEntries {
            upstream: "cached".into(),
            query: CacheQuery::default(),
        },
    )
    .await?
    .result
    .unwrap();
    assert_eq!(entries["cache"]["status"]["entries"], 0);
    for (key, all, expected) in [
        (None, false, 0),
        (Some(key.clone()), true, 0),
        (Some("bad".into()), false, 0),
        (None, true, 9),
    ] {
        assert!(request(
            &socket,
            Command::CachePurge {
                upstream: "cached".into(),
                key,
                all,
                expected_generation: expected
            }
        )
        .await
        .is_err());
    }
    for limit in [0, 1025] {
        assert!(request(
            &socket,
            Command::CacheEntries {
                upstream: "cached".into(),
                query: CacheQuery {
                    limit,
                    ..Default::default()
                }
            }
        )
        .await
        .is_err());
    }
    assert!(request(
        &socket,
        Command::CacheStatus {
            upstream: Some("missing".into())
        }
    )
    .await
    .is_err());
    assert!(request(
        &socket,
        Command::CachePurge {
            upstream: "disabled".into(),
            key: None,
            all: true,
            expected_generation: 0
        }
    )
    .await
    .is_err());
    assert_eq!(
        request(&socket, status()).await?.result.unwrap()["caches"]["cached"]["status"]["epoch"],
        0
    );
    let cleared = request(&socket, purge(0)).await?;
    assert_eq!(cleared.generation, 0);
    assert_eq!(cleared.result.as_ref().unwrap()["purge"]["epoch"], 1);
    assert_eq!(cleared.result.unwrap()["ephemeral"], true);
    request(
        &socket,
        Command::CachePurge {
            upstream: "cached".into(),
            key: Some(key),
            all: false,
            expected_generation: 0,
        },
    )
    .await?;
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(&mut io,&serde_json::json!({"version":1,"id":"missing-generation","method":"cache.purge","upstream":"cached","all":true})).await?;
    assert_eq!(
        timeout(Duration::from_secs(2), io.read(&mut [0])).await??,
        0
    );
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &Request {
            version: 1,
            id: "purge-disconnect".into(),
            command: purge(0),
        },
    )
    .await?;
    drop(io);
    timeout(Duration::from_secs(3), async {
        loop {
            if request(&socket, status()).await.unwrap().result.unwrap()["caches"]["cached"]
                ["status"]["epoch"]
                == 3
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(std::fs::read(&path)?, persisted);
    request(
        &socket,
        Command::Apply {
            config: config.clone(),
            expected_generation: 0,
        },
    )
    .await?;
    let response = request(&socket, status()).await?;
    assert_eq!(response.generation, 1);
    assert_eq!(
        response.result.unwrap()["caches"]["cached"]["status"]["epoch"],
        3
    );
    assert!(request(&socket, purge(0))
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_conflict"));
    let (applied, purged) = tokio::join!(
        request(
            &socket,
            Command::Apply {
                config,
                expected_generation: 1
            }
        ),
        request(&socket, purge(1))
    );
    assert_eq!(applied?.generation, 2);
    match purged {
        Ok(result) => assert_eq!(result.generation, 1),
        Err(error) => assert!(error.to_string().contains("generation_conflict")),
    };
    let mut io = UnixStream::connect(&socket).await?;
    write_frame(
        &mut io,
        &Request {
            version: 1,
            id: "read-state".into(),
            command: status(),
        },
    )
    .await?;
    let response: Response = read_frame(&mut io).await?;
    assert_eq!(response.generation, response.result.unwrap()["generation"]);
    server.shutdown().await;
    p.shutdown().await;
    Ok(())
}
