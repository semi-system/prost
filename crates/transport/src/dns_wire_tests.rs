use super::*;
#[tokio::test]
async fn wire_ttl_zero_and_srv_priority_port_are_respected() -> anyhow::Result<()> {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let calls = Arc::new(AtomicU64::new(0));
    let seen = calls.clone();
    let task = tokio::spawn(async move {
        let mut data = [0u8; 4096];
        while let Ok((length, peer)) = socket.recv_from(&mut data).await {
            let mut end = 12;
            while end < length && data[end] != 0 {
                end += usize::from(data[end]) + 1;
            }
            end += 1;
            if end + 4 > length {
                continue;
            }
            let kind = u16::from_be_bytes([data[end], data[end + 1]]);
            let mut reply = data[..end + 4].to_vec();
            reply[2..4].copy_from_slice(&[0x81, 0x80]);
            reply[6..12].fill(0);
            let mut answers = Vec::new();
            if kind == 1 {
                let count = seen.fetch_add(1, Ordering::SeqCst);
                answers.push((1u16, vec![127, 0, 0, 1 + u8::try_from(count % 2).unwrap()]));
            }
            if kind == 33 {
                for (priority, port) in [(10u16, 2525u16), (20, 9999)] {
                    let mut value = Vec::new();
                    value.extend_from_slice(&priority.to_be_bytes());
                    value.extend_from_slice(&1u16.to_be_bytes());
                    value.extend_from_slice(&port.to_be_bytes());
                    value.extend_from_slice(b"\x06target\x04test\0");
                    answers.push((33, value));
                }
            }
            reply[6..8].copy_from_slice(&(answers.len() as u16).to_be_bytes());
            for (kind, value) in answers {
                reply.extend_from_slice(&[0xc0, 0x0c]);
                reply.extend_from_slice(&kind.to_be_bytes());
                reply.extend_from_slice(&1u16.to_be_bytes());
                reply.extend_from_slice(&0u32.to_be_bytes());
                reply.extend_from_slice(&(value.len() as u16).to_be_bytes());
                reply.extend_from_slice(&value);
            }
            let _ = socket.send_to(&reply, peer).await;
        }
    });
    let dns = DnsResolver::new(DnsPolicy {
        authoritative_ttl: true,
        nameservers: vec![address],
        timeout_ms: 1000,
        ..Default::default()
    })?;
    assert_ne!(
        dns.resolve("zero.test.").await?,
        dns.resolve("zero.test.").await?
    );
    assert!(calls.load(Ordering::SeqCst) >= 2);
    for _ in 0..4 {
        let values = dns.endpoint("_smtp._tcp.example.test:1").await?;
        assert_eq!(values[0].port(), 2525);
        assert_eq!(values.last().unwrap().port(), 9999);
    }
    task.abort();
    Ok(())
}
