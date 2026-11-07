use anyhow::Result;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use prost_config::{Config, Endpoint, Protocol};
use prost_core::{ProxyBuilder, RunningProxy};
use std::{convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn echo() -> Server {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((mut io, _)) = l.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = io.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    Server { address, task }
}
async fn http_server(label: &'static str) -> Server {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    let seen = request.headers().get("x-test-plugin").cloned();
                    let forwarded = request.headers().get("x-forwarded-for").cloned();
                    let body: http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible> =
                        if request.uri().path() == "/grpc" {
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                            let frames = vec![
                                Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                                    b"\0\0\0\0\x04test",
                                ))),
                                Ok(Frame::trailers(trailers)),
                            ];
                            StreamBody::new(futures_util::stream::iter(frames))
                                .map_err(|e| match e {})
                                .boxed_unsync()
                        } else {
                            Full::new(Bytes::from_static(label.as_bytes()))
                                .map_err(|e| match e {})
                                .boxed_unsync()
                        };
                    let mut response = Response::new(body);
                    response.headers_mut().insert(
                        "content-type",
                        http::HeaderValue::from_static("application/grpc"),
                    );
                    if let Some(v) = seen {
                        response.headers_mut().insert("x-seen-plugin", v);
                    }
                    if let Some(v) = forwarded {
                        response.headers_mut().insert("x-seen-forwarded", v);
                    }
                    Ok::<_, Infallible>(response)
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    Server { address, task }
}
fn free_address() -> SocketAddr {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}
struct Fixture {
    dir: TempDir,
    path: PathBuf,
    config: Config,
}
impl Fixture {
    fn new(protocol: Protocol, endpoint: SocketAddr) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prost.toml");
        let text = format!(
            r#"
schema_version=1
[runtime]
admin_socket={socket:?}
artifact_dir={artifacts:?}
drain_seconds=1
[upstreams.main]
health_interval_ms=100
idle_timeout_ms=2000
[[upstreams.main.endpoints]]
id="one"
address={endpoint:?}
[[listeners]]
id="main"
address={address:?}
protocol={protocol:?}
upstream="main"
"#,
            socket = dir.path().join("admin.sock").to_str().unwrap(),
            artifacts = dir.path().join("artifacts").to_str().unwrap(),
            endpoint = endpoint.to_string(),
            address = free_address().to_string(),
            protocol = if protocol == Protocol::Tcp {
                "tcp"
            } else {
                "http"
            }
        );
        let config = Config::parse(&text).unwrap();
        std::fs::write(&path, config.encode().unwrap()).unwrap();
        Self { dir, path, config }
    }
    async fn start(&self) -> Arc<RunningProxy> {
        ProxyBuilder::new(&self.path)
            .unwrap()
            .start()
            .await
            .unwrap()
    }
    fn address(&self) -> SocketAddr {
        self.config.listeners[0].address
    }
}
async fn roundtrip(
    io: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    data: &[u8],
) -> Result<()> {
    timeout(Duration::from_secs(3), async {
        io.write_all(data).await?;
        let mut response = vec![0; data.len()];
        io.read_exact(&mut response).await?;
        assert_eq!(response, data);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn tcp_update_preserves_session_and_half_close() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let mut old = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut old, b"before").await?;
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].enabled = false;
    p.apply(next.encode()?, 0).await?;
    roundtrip(&mut old, b"after").await?;
    let mut new = TcpStream::connect(fixture.address()).await?;
    new.write_all(b"blocked").await?;
    let mut buf = [0; 16];
    assert!(matches!(
        timeout(Duration::from_secs(3), new.read(&mut buf)).await?,
        Ok(0) | Err(_)
    ));
    old.write_all(b"half-close").await?;
    old.shutdown().await?;
    let mut last = Vec::new();
    timeout(Duration::from_secs(3), old.read_to_end(&mut last)).await??;
    assert_eq!(last, b"half-close");
    drop(old);
    drop(new);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn transaction_conflict_bind_failure_and_restart() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await?;
    let before = std::fs::read(&fixture.path)?;
    let mut bad = p.snapshot().config.clone();
    let mut listener = bad.listeners[0].clone();
    listener.id = "occupied".into();
    listener.address = occupied.local_addr()?;
    bad.listeners.push(listener);
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(std::fs::read(&fixture.path)?, before);
    assert_eq!(p.snapshot().config.generation, 0);
    let config = p.snapshot().config.encode()?;
    let (a, b) = tokio::join!(p.apply(config.clone(), 0), p.apply(config, 0));
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(p.apply("schema_version = 99".into(), 1).await.is_err());
    assert_eq!(p.snapshot().config.generation, 1);
    let hash = p.snapshot().hash.clone();
    p.shutdown().await;
    drop(p);
    let p = fixture.start().await;
    assert_eq!(p.snapshot().config.generation, 1);
    assert_eq!(p.snapshot().hash, hash);
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"restart").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn file_edits_do_not_change_running_config() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    std::fs::write(&fixture.path, "invalid config")?;
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"no watcher").await?;
    assert_eq!(p.snapshot().config.generation, 0);
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn keepalive_requests_use_new_generation_and_release_old() -> Result<()> {
    let a = http_server("old").await;
    let b = http_server("new").await;
    let fixture = Fixture::new(Protocol::Http, a.address);
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let send = |label: &str| {
        Request::builder()
            .uri("/")
            .header("host", label)
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let old = Arc::downgrade(&p.snapshot());
    let response = client.send_request(send("localhost")).await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "old");
    let mut config = p.snapshot().config.clone();
    config.upstreams.get_mut("main").unwrap().endpoints[0].address = b.address.to_string();
    p.apply(config.encode()?, 0).await?;
    let response = client.send_request(send("localhost")).await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "new");
    timeout(Duration::from_secs(3), async {
        while old.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn http2_grpc_trailers_and_untrusted_forwarded_headers() -> Result<()> {
    let backend = http_server("h2").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.upstreams.get_mut("main").unwrap().http2 = true;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let request = Request::builder()
        .uri(format!("http://{}/grpc", fixture.address()))
        .header("te", "trailers")
        .header("x-forwarded-for", "evil")
        .body(Full::new(Bytes::new()))?;
    let response = client.send_request(request).await?;
    assert_eq!(response.headers()["x-seen-forwarded"], "127.0.0.1");
    let body = response.into_body().collect().await?;
    assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(body.to_bytes(), Bytes::from_static(b"\0\0\0\0\x04test"));
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

fn certificate(dir: &std::path::Path) -> Result<prost_config::ServerTls> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let certificate = dir.join("cert.pem");
    let private_key = dir.join("key.pem");
    std::fs::write(&certificate, cert.cert.pem())?;
    std::fs::write(&private_key, cert.key_pair.serialize_pem())?;
    Ok(prost_config::ServerTls {
        certificate,
        private_key,
    })
}
#[tokio::test]
async fn tls_termination_rotation_and_artifact_restart() -> Result<()> {
    let backend = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, backend.address);
    let tls = certificate(fixture.dir.path())?;
    fixture.config.listeners[0].tls = Some(tls.clone());
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let client = prost_transport::client_tls(Some(&prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate.clone()),
    }))?;
    let connector = tokio_rustls::TlsConnector::from(client);
    let mut io = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"tls old").await?;
    let old_cert = io.get_ref().1.peer_certificates().unwrap()[0].clone();
    let replacement_dir = fixture.dir.path().join("replacement");
    std::fs::create_dir(&replacement_dir)?;
    let replacement = certificate(&replacement_dir)?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].tls = Some(replacement.clone());
    p.apply(config.encode()?, 0).await?;
    roundtrip(&mut io, b"still old session").await?;
    let client = prost_transport::client_tls(Some(&prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(replacement.certificate.clone()),
    }))?;
    let connector = tokio_rustls::TlsConnector::from(client);
    let mut new = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    assert_ne!(new.get_ref().1.peer_certificates().unwrap()[0], old_cert);
    roundtrip(&mut new, b"new cert").await?;
    // Source files can disappear; committed artifacts remain usable after restart.
    std::fs::remove_file(replacement.certificate)?;
    std::fs::remove_file(replacement.private_key)?;
    drop(io);
    drop(new);
    p.shutdown().await;
    drop(p);
    let p = fixture.start().await;
    let mut io = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"artifact").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tls_upstream_verified_and_bad_certificate_rejected() -> Result<()> {
    let backend = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, backend.address);
    let tls = certificate(fixture.dir.path())?;
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, false)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut io) = acceptor.accept(io).await {
                    let (mut r, mut w) = tokio::io::split(&mut io);
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                }
            });
        }
    });
    let server = Server { address, task };
    let upstream = fixture.config.upstreams.get_mut("main").unwrap();
    upstream.endpoints[0].address = server.address.to_string();
    upstream.tls = Some(prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate),
    });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"verified TLS upstream").await?;
    let mut bad = p.snapshot().config.clone();
    bad.listeners[0].tls = Some(prost_config::ServerTls {
        certificate: fixture.dir.path().join("missing"),
        private_key: fixture.dir.path().join("missing-key"),
    });
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn websocket_tunnel_survives_config_update() -> Result<()> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let address = l.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(io).await {
                    while let Some(Ok(message)) = ws.next().await {
                        if ws.send(message).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });
    let server = Server { address, task };
    let fixture = Fixture::new(Protocol::Http, server.address);
    let p = fixture.start().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/", fixture.address())).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("before"))
        .await?;
    assert_eq!(
        timeout(Duration::from_secs(3), ws.next())
            .await?
            .unwrap()?
            .into_text()?,
        "before"
    );
    p.set_enabled("main/one".into(), false, 0).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("after"))
        .await?;
    assert_eq!(
        timeout(Duration::from_secs(3), ws.next())
            .await?
            .unwrap()?
            .into_text()?,
        "after"
    );
    drop(ws);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn listener_add_remove_and_force_shutdown() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let mut config = p.snapshot().config.clone();
    let mut second = config.listeners[0].clone();
    second.id = "second".into();
    second.address = free_address();
    let address = second.address;
    config.listeners.push(second);
    p.apply(config.encode()?, 0).await?;
    let mut old = TcpStream::connect(address).await?;
    roundtrip(&mut old, b"added").await?;
    let mut config = p.snapshot().config.clone();
    config.listeners.retain(|l| l.id != "second");
    p.apply(config.encode()?, 1).await?;
    roundtrip(&mut old, b"removed still live").await?;
    // The live stream forces the configured one-second drain deadline.
    timeout(Duration::from_secs(3), p.shutdown()).await?;
    let mut b = [0];
    assert_eq!(old.read(&mut b).await?, 0);
    Ok(())
}

#[tokio::test]
async fn wasm_header_plugin_updates_and_rejects_invalid_module() -> Result<()> {
    let backend = http_server("plugin").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fn module(value: &str) -> String {
        let output = format!(r#"{{"set_headers":{{"x-test-plugin":"{value}"}}}}"#);
        let bytes = output
            .as_bytes()
            .iter()
            .map(|b| format!("\\{b:02x}"))
            .collect::<String>();
        let packed = ((1024u64) << 32) | output.len() as u64;
        format!(
            r#"(module (memory (export "memory") 1) (data (i32.const 1024) "{bytes}") (func (export "prost_abi_version") (result i32) i32.const 1) (func (export "alloc") (param i32) (result i32) i32.const 4096) (func (export "handle") (param i32 i32) (result i64) i64.const {packed}))"#
        )
    }
    let wasm = fixture.dir.path().join("plugin.wat");
    std::fs::write(&wasm, module("v1"))?;
    fixture.config.listeners[0]
        .plugins
        .push(prost_config::Plugin {
            name: "wasm".into(),
            module: Some(wasm.clone()),
            config: Default::default(),
        });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    async fn check(address: SocketAddr, expected: &str) -> Result<()> {
        let io = TcpStream::connect(address).await?;
        let (mut client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-seen-plugin"], expected);
        response.into_body().collect().await?;
        drop(client);
        driver.abort();
        Ok(())
    }
    check(fixture.address(), "v1").await?;
    std::fs::write(&wasm, module("v2"))?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[0].module = Some(wasm.clone());
    p.apply(config.encode()?, 0).await?;
    check(fixture.address(), "v2").await?;
    std::fs::write(&wasm, "invalid wasm")?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[0].module = Some(wasm);
    assert!(p.apply(config.encode()?, 1).await.is_err());
    check(fixture.address(), "v2").await?;
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn weighted_round_robin_and_endpoint_disable() -> Result<()> {
    let a = http_server("a").await;
    let b = http_server("b").await;
    let mut fixture = Fixture::new(Protocol::Http, a.address);
    let u = fixture.config.upstreams.get_mut("main").unwrap();
    u.endpoints[0].weight = 2;
    u.endpoints.push(Endpoint {
        id: "two".into(),
        address: b.address.to_string(),
        weight: 1,
        enabled: true,
    });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let mut results = Vec::new();
    for _ in 0..6 {
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        results.push(response.into_body().collect().await?.to_bytes());
    }
    assert_eq!(results.iter().filter(|v| v.as_ref() == b"a").count(), 4);
    assert_eq!(results.iter().filter(|v| v.as_ref() == b"b").count(), 2);
    p.set_enabled("main/one".into(), false, 0).await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "b");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
