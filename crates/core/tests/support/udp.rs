use super::*;
use tokio::net::UdpSocket;

async fn backend(label: &'static [u8]) -> Result<Server> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let task = tokio::spawn(async move {
        let mut buffer = vec![0; 65536];
        while let Ok((n, peer)) = socket.recv_from(&mut buffer).await {
            let mut result = label.to_vec();
            result.extend_from_slice(&buffer[..n]);
            let _ = socket.send_to(&result, peer).await;
        }
    });
    Ok(Server { address, task })
}
fn fixture(address: SocketAddr) -> Result<Fixture> {
    let mut f = Fixture::new(Protocol::Http, address);
    let temporary = std::net::UdpSocket::bind("127.0.0.1:0")?;
    f.config.listeners[0].address = temporary.local_addr()?;
    f.config.listeners[0].protocol = Protocol::Udp;
    f.config.listeners[0].udp = Some(prost_config::UdpSettings {
        idle_timeout_ms: 2000,
        ..Default::default()
    });
    f.config
        .upstreams
        .get_mut("main")
        .unwrap()
        .health_check
        .enabled = false;
    std::fs::write(&f.path, f.config.encode()?)?;
    Ok(f)
}
async fn exchange(client: &UdpSocket, address: SocketAddr, data: &[u8]) -> Result<Vec<u8>> {
    client.send_to(data, address).await?;
    let mut response = vec![0; 65536];
    let (n, source) = timeout(Duration::from_secs(3), client.recv_from(&mut response)).await??;
    assert_eq!(source, address);
    response.truncate(n);
    Ok(response)
}
async fn drained(p: &RunningProxy) -> Result<()> {
    timeout(Duration::from_secs(5), async {
        while p.metrics.connections.get() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_binary_zero_length_and_tcp_can_share_port() -> Result<()> {
    let server = backend(b"").await?;
    let tcp = echo().await;
    let mut f = fixture(server.address)?;
    let mut upstream = f.config.upstreams["main"].clone();
    upstream.endpoints[0].address = tcp.address.to_string();
    f.config.upstreams.insert("tcp".into(), upstream);
    let mut listener = f.config.listeners[0].clone();
    listener.id = "tcp".into();
    listener.protocol = Protocol::Tcp;
    listener.udp = None;
    listener.upstream = Some("tcp".into());
    f.config.listeners.push(listener);
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let client = UdpSocket::bind("127.0.0.1:0").await?;
    for data in [vec![], vec![0, 255, 1, 0], vec![b'x'; 64000]] {
        assert_eq!(exchange(&client, f.address(), &data).await?, data);
    }
    let mut io = TcpStream::connect(f.address()).await?;
    roundtrip(&mut io, b"same TCP port").await?;
    drop(io);
    p.shutdown().await;
    assert_eq!(p.metrics.connections.get(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_rolling_preserves_peer_backend_and_new_peers_use_new_generation() -> Result<()> {
    let blue = backend(b"blue:").await?;
    let green = backend(b"green:").await?;
    let mut f = fixture(blue.address)?;
    f.config.runtime.reload_mode = prost_config::ReloadMode::Rolling;
    f.config.runtime.drain_seconds = 0;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let old = UdpSocket::bind("127.0.0.1:0").await?;
    assert_eq!(exchange(&old, f.address(), b"one").await?, b"blue:one");
    let mut bad = p.snapshot().config.clone();
    bad.listeners[0].protocol = Protocol::Tcp;
    bad.listeners[0].udp = None;
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    let occupied = UdpSocket::bind("127.0.0.1:0").await?;
    let mut bad = p.snapshot().config.clone();
    let mut listener = bad.listeners[0].clone();
    listener.id = "occupied".into();
    listener.address = occupied.local_addr()?;
    bad.listeners.push(listener);
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(
        exchange(&old, f.address(), b"rollback").await?,
        b"blue:rollback"
    );
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].address = green.address.to_string();
    p.apply(next.encode()?, 0).await?;
    assert_eq!(exchange(&old, f.address(), b"two").await?, b"blue:two");
    let new = UdpSocket::bind("127.0.0.1:0").await?;
    assert_eq!(exchange(&new, f.address(), b"three").await?, b"green:three");
    assert_eq!(p.status()["worker_generations"]["forced_total"], 0);
    drained(&p).await?;
    p.wait_for_workers(1, 3000).await?;
    assert_eq!(p.status()["worker_generations"]["forced_total"], 0);
    assert_eq!(
        exchange(&old, f.address(), b"after-idle").await?,
        b"green:after-idle"
    );
    p.shutdown().await;
    drop(p);
    let p = f.start().await;
    assert_eq!(
        exchange(&old, f.address(), b"restart").await?,
        b"green:restart"
    );
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_limits_oversize_and_idle_release_are_bounded() -> Result<()> {
    let server = backend(b"").await?;
    let mut f = fixture(server.address)?;
    let l = &mut f.config.listeners[0];
    l.max_connections_per_ip = 1;
    l.udp = Some(prost_config::UdpSettings {
        idle_timeout_ms: 300,
        max_datagram_bytes: 16,
        session_queue_packets: 1,
        ..Default::default()
    });
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let one = UdpSocket::bind("127.0.0.1:0").await?;
    let two = UdpSocket::bind("127.0.0.1:0").await?;
    assert_eq!(exchange(&one, f.address(), b"ok").await?, b"ok");
    two.send_to(b"denied", f.address()).await?;
    let mut data = [0; 64];
    assert!(timeout(Duration::from_millis(80), two.recv(&mut data))
        .await
        .is_err());
    one.send_to(&[0; 17], f.address()).await?;
    assert!(timeout(Duration::from_millis(80), one.recv(&mut data))
        .await
        .is_err());
    assert_eq!(
        metric(&p, "prost_udp_dropped_total", &[("reason", "oversize")]),
        Some(1.)
    );
    assert_eq!(
        metric(
            &p,
            "prost_udp_dropped_total",
            &[("reason", "session_capacity")]
        ),
        Some(1.)
    );
    drained(&p).await?;
    assert_eq!(exchange(&two, f.address(), b"released").await?, b"released");
    p.shutdown().await;
    assert_eq!(p.metrics.connections.get(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_query_sessions_balance_bursts_without_mixing_peer_responses() -> Result<()> {
    let blue = backend(b"blue:").await?;
    let green = backend(b"green:").await?;
    let mut f = fixture(blue.address)?;
    let u = f.config.upstreams.get_mut("main").unwrap();
    let mut endpoint = u.endpoints[0].clone();
    endpoint.id = "two".into();
    endpoint.address = green.address.to_string();
    u.endpoints.push(endpoint);
    f.config.listeners[0].udp.as_mut().unwrap().session_requests = 1;
    f.config.listeners[0]
        .udp
        .as_mut()
        .unwrap()
        .session_responses = 1;
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let clients = [
        UdpSocket::bind("127.0.0.1:0").await?,
        UdpSocket::bind("127.0.0.1:0").await?,
    ];
    for (client_id, client) in clients.iter().enumerate() {
        for i in 0..10 {
            client
                .send_to(format!("{client_id}:{i}").as_bytes(), f.address())
                .await?;
        }
    }
    let mut total_blue = 0;
    for (client_id, client) in clients.iter().enumerate() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let mut buffer = [0; 128];
            let (n, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buffer)).await??;
            let reply = std::str::from_utf8(&buffer[..n])?;
            let (backend, payload) = reply.split_once(':').unwrap();
            if backend == "blue" {
                total_blue += 1;
            } else {
                assert_eq!(backend, "green");
            }
            assert!(
                payload.starts_with(&format!("{client_id}:")),
                "response leaked between clients: {reply}"
            );
            assert!(seen.insert(payload.to_owned()));
        }
        assert_eq!(seen.len(), 10);
    }
    assert_eq!(total_blue, 10);
    drained(&p).await?;
    assert_eq!(p.status()["udp"]["buffer_reserved_bytes"], 0);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn udp_connected_backend_socket_rejects_unsolicited_other_sources() -> Result<()> {
    let server = UdpSocket::bind("127.0.0.1:0").await?;
    let (peer_tx, peer_rx) = tokio::sync::oneshot::channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let ready = release.clone();
    let f = fixture(server.local_addr()?)?;
    let task = tokio::spawn(async move {
        let mut buffer = [0; 128];
        let (_, peer) = server.recv_from(&mut buffer).await.unwrap();
        let _ = peer_tx.send(peer);
        ready.notified().await;
        server.send_to(b"real-backend", peer).await.unwrap();
    });
    let p = f.start().await;
    let client = UdpSocket::bind("127.0.0.1:0").await?;
    client.send_to(b"request", f.address()).await?;
    let proxy_backend = timeout(Duration::from_secs(3), peer_rx).await??;
    let attacker = UdpSocket::bind("127.0.0.1:0").await?;
    attacker.send_to(b"forged", proxy_backend).await?;
    let mut buffer = [0; 128];
    assert!(
        timeout(Duration::from_millis(100), client.recv(&mut buffer))
            .await
            .is_err()
    );
    release.notify_one();
    let n = timeout(Duration::from_secs(3), client.recv(&mut buffer)).await??;
    assert_eq!(&buffer[..n], b"real-backend");
    task.await?;
    p.shutdown().await;
    Ok(())
}
