use anyhow::{bail, ensure, Result};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::io::{AsyncRead, AsyncReadExt};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyAddresses {
    pub source: SocketAddr,
    pub destination: SocketAddr,
}

/// Fixed-size binary TCP address header. No caller-supplied bytes or TLVs.
pub async fn write_proxy_v2(
    io: &mut (impl tokio::io::AsyncWrite + Unpin),
    addresses: ProxyAddresses,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let source = addresses.source;
    let destination = addresses.destination;
    let mut frame = [0u8; 52];
    frame[..12].copy_from_slice(b"\r\n\r\n\0\r\nQUIT\n");
    frame[12] = 0x21; // v2, PROXY
    let length = match (source.ip(), destination.ip()) {
        (IpAddr::V4(src), IpAddr::V4(dst)) => {
            frame[13] = 0x11;
            frame[14..16].copy_from_slice(&12u16.to_be_bytes());
            frame[16..20].copy_from_slice(&src.octets());
            frame[20..24].copy_from_slice(&dst.octets());
            frame[24..26].copy_from_slice(&source.port().to_be_bytes());
            frame[26..28].copy_from_slice(&destination.port().to_be_bytes());
            28
        }
        (src, dst) => {
            // Mixed families can occur after a trusted ingress PROXY header.
            // Encode IPv4 as IPv4-mapped IPv6 rather than losing the client IP.
            let v6 = |ip: IpAddr| match ip {
                IpAddr::V4(ip) => ip.to_ipv6_mapped(),
                IpAddr::V6(ip) => ip,
            };
            frame[13] = 0x21;
            frame[14..16].copy_from_slice(&36u16.to_be_bytes());
            frame[16..32].copy_from_slice(&v6(src).octets());
            frame[32..48].copy_from_slice(&v6(dst).octets());
            frame[48..50].copy_from_slice(&source.port().to_be_bytes());
            frame[50..52].copy_from_slice(&destination.port().to_be_bytes());
            52
        }
    };
    io.write_all(&frame[..length]).await?;
    Ok(())
}

/// Consume exactly one mandatory PROXY v1/v2 header before TLS/protocol parsing.
pub async fn read_proxy(
    io: &mut (impl AsyncRead + Unpin),
    peer: SocketAddr,
    config: &prost_config::ProxyProtocol,
) -> Result<SocketAddr> {
    Ok(read_proxy_addresses(io, peer, peer, config).await?.source)
}
pub async fn read_proxy_addresses(
    io: &mut (impl AsyncRead + Unpin),
    peer: SocketAddr,
    destination: SocketAddr,
    config: &prost_config::ProxyProtocol,
) -> Result<ProxyAddresses> {
    ensure!(
        config.trusted_peers.iter().any(|n| n.contains(&peer.ip())),
        "untrusted PROXY protocol peer"
    );
    tokio::time::timeout(
        std::time::Duration::from_millis(config.timeout_ms),
        parse(
            io,
            ProxyAddresses {
                source: peer,
                destination,
            },
        ),
    )
    .await?
}
async fn parse(io: &mut (impl AsyncRead + Unpin), peer: ProxyAddresses) -> Result<ProxyAddresses> {
    let mut start = [0; 6];
    io.read_exact(&mut start).await?;
    if &start == b"PROXY " {
        let mut line = Vec::with_capacity(108);
        loop {
            ensure!(line.len() < 102, "PROXY v1 line exceeds 108 bytes");
            line.push(io.read_u8().await?);
            if line.ends_with(b"\r\n") {
                break;
            }
        }
        let fields: Vec<_> = std::str::from_utf8(&line)?
            .split_ascii_whitespace()
            .collect();
        if fields.as_slice() == ["UNKNOWN"] {
            return Ok(peer);
        }
        ensure!(
            fields.len() == 5 && matches!(fields[0], "TCP4" | "TCP6"),
            "invalid PROXY v1 header"
        );
        let source: IpAddr = fields[1].parse()?;
        let dest: IpAddr = fields[2].parse()?;
        ensure!(
            source.is_ipv4() == (fields[0] == "TCP4") && source.is_ipv4() == dest.is_ipv4(),
            "PROXY address family mismatch"
        );
        let port: u16 = fields[3].parse()?;
        let dest_port: u16 = fields[4].parse()?;
        return Ok(ProxyAddresses {
            source: SocketAddr::new(source, port),
            destination: SocketAddr::new(dest, dest_port),
        });
    }
    ensure!(&start == b"\r\n\r\n\0\r", "missing PROXY header");
    let mut rest = [0; 10];
    io.read_exact(&mut rest).await?;
    ensure!(
        &rest[..6] == b"\nQUIT\n" && rest[6] >> 4 == 2,
        "invalid PROXY v2 signature/version"
    );
    let len = u16::from_be_bytes([rest[8], rest[9]]) as usize;
    ensure!(len <= 512, "PROXY v2 payload exceeds 512 bytes");
    let mut data = vec![0; len];
    io.read_exact(&mut data).await?;
    match rest[6] & 15 {
        0 => Ok(peer), // LOCAL: ignore metadata.
        1 => match rest[7] {
            0x11 if len >= 12 => Ok(ProxyAddresses {
                source: SocketAddr::new(
                    Ipv4Addr::new(data[0], data[1], data[2], data[3]).into(),
                    u16::from_be_bytes([data[8], data[9]]),
                ),
                destination: SocketAddr::new(
                    Ipv4Addr::new(data[4], data[5], data[6], data[7]).into(),
                    u16::from_be_bytes([data[10], data[11]]),
                ),
            }),
            0x21 if len >= 36 => Ok(ProxyAddresses {
                source: SocketAddr::new(
                    Ipv6Addr::from(<[u8; 16]>::try_from(&data[..16])?).into(),
                    u16::from_be_bytes([data[32], data[33]]),
                ),
                destination: SocketAddr::new(
                    Ipv6Addr::from(<[u8; 16]>::try_from(&data[16..32])?).into(),
                    u16::from_be_bytes([data[34], data[35]]),
                ),
            }),
            _ => bail!("unsupported PROXY v2 family/transport"),
        },
        _ => bail!("invalid PROXY v2 command"),
    }
}
