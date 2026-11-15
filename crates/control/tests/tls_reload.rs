use anyhow::Result;
use prost_control::{request, write_frame, Command, ControlServer, Request};
use prost_core::{prost_config, ProxyBuilder};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UnixStream},
    time::{timeout, Duration},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_reload_wire_disconnect_conflict_and_regular_apply_share_coordinator() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let config_path = dir.path().join("prost.toml");
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
    let cert = rcgen::generate_simple_self_signed(vec!["mail.test".into()])?;
    let certificate = dir.path().join("cert.pem");
    let private_key = dir.path().join("key.pem");
    std::fs::write(&certificate, cert.cert.pem())?;
    std::fs::write(&private_key, cert.key_pair.serialize_pem())?;
    let reserved = TcpListener::bind("127.0.0.1:0").await?;
    let address = reserved.local_addr()?;
    drop(reserved);
    let mut config = prost_config::Config::parse(&format!("schema_version=1\n[runtime]\nadmin_socket={socket:?}\nartifact_dir={:?}\nreload_mode='rolling'\nio_workers=1\ndrain_seconds=1\n[[upstreams.mail.endpoints]]\nid='one'\naddress='{backend_address}'\n[[listeners]]\nid='plain'\naddress='{address}'\nprotocol='tcp'\nupstream='mail'\n",dir.path().join("artifacts")))?;
    let mut secure = config.listeners[0].clone();
    secure.id = "secure".into();
    let reserved = TcpListener::bind("127.0.0.1:0").await?;
    secure.address = reserved.local_addr()?;
    drop(reserved);
    let tls = prost_config::ServerTls {
        certificate,
        private_key,
        ..Default::default()
    };
    secure.tls = Some(tls.clone());
    config.listeners.push(secure);
    std::fs::write(&config_path, config.encode()?)?;
    let proxy = ProxyBuilder::new(&config_path)?.start().await?;
    let control = ControlServer::bind(proxy.clone()).await?;
    let mut held = TcpStream::connect(address).await?;
    held.write_all(b"a").await?;
    assert_eq!(held.read_u8().await?, b'a');
    let mut client = UnixStream::connect(&socket).await?;
    write_frame(
        &mut client,
        &Request {
            version: 1,
            id: "tls-disconnect".into(),
            command: Command::ReloadTls {
                listener: "secure".into(),
                tls: Box::new(tls.clone()),
                expected_generation: 0,
            },
        },
    )
    .await?;
    drop(client);
    timeout(Duration::from_secs(5), async {
        while proxy.snapshot().config.generation != 1 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    assert_eq!(proxy.status()["worker_generations"]["retired_total"], 0);
    assert!(request(
        &socket,
        Command::ReloadTls {
            listener: "secure".into(),
            tls: Box::new(tls),
            expected_generation: 0
        }
    )
    .await
    .unwrap_err()
    .to_string()
    .contains("generation_conflict"));
    tokio::time::sleep(Duration::from_millis(1100)).await;
    held.write_all(b"b").await?;
    assert_eq!(held.read_u8().await?, b'b');
    drop(held);
    let result = request(
        &socket,
        Command::Apply {
            config: proxy.snapshot().config.encode()?,
            expected_generation: 1,
        },
    )
    .await?;
    assert_eq!(result.generation, 2);
    request(
        &socket,
        Command::WaitWorkers {
            generation: 2,
            timeout_ms: 3000,
        },
    )
    .await?;
    assert_eq!(proxy.status()["worker_generations"]["retired_total"], 1);
    control.shutdown().await;
    proxy.shutdown().await;
    backend_task.abort();
    Ok(())
}
