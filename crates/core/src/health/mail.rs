//! Bounded application probes; STARTTLS is a health-check capability only.
use anyhow::{ensure, Context, Result};
use prost_config::{ClientTls, HealthCheck, HealthProtocol};
use prost_transport::BoxStream;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

type Reader = BufReader<BoxStream>;
async fn line(io: &mut Reader, budget: &mut usize, limit: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(128);
    loop {
        ensure!(
            *budget > 0 && out.len() < limit,
            "mail health reply exceeds byte budget"
        );
        let byte = io.read_u8().await?;
        *budget -= 1;
        out.push(byte);
        if byte == b'\n' {
            ensure!(out.ends_with(b"\r\n"), "mail reply must end in CRLF");
            out.truncate(out.len() - 2);
            return Ok(out);
        }
    }
}
async fn send(io: &mut Reader, command: &[u8]) -> Result<()> {
    io.write_all(command).await?;
    io.flush().await?;
    Ok(())
}
async fn smtp(io: &mut Reader, code: u16, budget: &mut usize) -> Result<Vec<Vec<u8>>> {
    let expected = code.to_string();
    let mut lines = Vec::new();
    for _ in 0..64 {
        let value = line(io, budget, 512).await?;
        ensure!(
            value.len() >= 4
                && value[..3] == *expected.as_bytes()
                && matches!(value[3], b' ' | b'-'),
            "unexpected SMTP health reply"
        );
        let last = value[3] == b' ';
        lines.push(value[4..].to_vec());
        if last {
            return Ok(lines);
        }
    }
    anyhow::bail!("too many SMTP reply lines")
}
fn token(line: &[u8], expected: &[u8]) -> bool {
    line.split(|b| b.is_ascii_whitespace())
        .next()
        .is_some_and(|v| v.eq_ignore_ascii_case(expected))
}
async fn upgrade(
    io: Reader,
    config: Option<&ClientTls>,
    tls: Arc<rustls::ClientConfig>,
) -> Result<Reader> {
    ensure!(
        io.buffer().is_empty(),
        "unexpected plaintext after STARTTLS reply"
    );
    ensure!(
        config.is_some(),
        "STARTTLS requires TLS verification settings"
    );
    Ok(BufReader::new(
        prost_transport::upgrade_tls(io.into_inner(), config, tls)
            .await
            .context("mail health STARTTLS handshake")?,
    ))
}
async fn imap(io: &mut Reader, tag: &[u8], budget: &mut usize) -> Result<bool> {
    let mut starttls = false;
    for _ in 0..64 {
        let value = line(io, budget, 4096).await?;
        let mut parts = value
            .split(|b| b.is_ascii_whitespace())
            .filter(|s| !s.is_empty());
        let first = parts.next().unwrap_or_default();
        let status = parts.next().unwrap_or_default();
        if first.eq_ignore_ascii_case(tag) {
            ensure!(
                status.eq_ignore_ascii_case(b"OK"),
                "IMAP health command failed"
            );
            return Ok(starttls);
        }
        ensure!(first == b"*", "unexpected IMAP health tag");
        if status.eq_ignore_ascii_case(b"CAPABILITY") {
            starttls |= parts.any(|s| s.eq_ignore_ascii_case(b"STARTTLS"));
        }
        ensure!(
            !status.eq_ignore_ascii_case(b"BYE"),
            "IMAP closed health session"
        );
    }
    anyhow::bail!("too many IMAP health reply lines")
}
async fn pop3(io: &mut Reader, budget: &mut usize) -> Result<()> {
    ensure!(
        token(&line(io, budget, 4096).await?, b"+OK"),
        "POP3 health command failed"
    );
    Ok(())
}

pub(super) async fn probe(
    stream: BoxStream,
    policy: &HealthCheck,
    config: Option<&ClientTls>,
    tls: Arc<rustls::ClientConfig>,
) -> Result<bool> {
    let mut io = BufReader::new(stream);
    let mut budget = 65536;
    match policy.protocol {
        HealthProtocol::Smtp | HealthProtocol::SmtpStarttls => {
            smtp(&mut io, 220, &mut budget).await?;
            if policy.protocol == HealthProtocol::Smtp {
                return Ok(true);
            }
            let ehlo = format!(
                "EHLO {}\r\n",
                policy.host.as_deref().unwrap_or("prost-health.invalid")
            );
            send(&mut io, ehlo.as_bytes()).await?;
            ensure!(
                smtp(&mut io, 250, &mut budget)
                    .await?
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(b"STARTTLS")),
                "SMTP STARTTLS capability absent"
            );
            send(&mut io, b"STARTTLS\r\n").await?;
            smtp(&mut io, 220, &mut budget).await?;
            io = upgrade(io, config, tls).await?;
            send(&mut io, ehlo.as_bytes()).await?;
            smtp(&mut io, 250, &mut budget).await?;
            send(&mut io, b"QUIT\r\n").await?;
            smtp(&mut io, 221, &mut budget).await?;
        }
        HealthProtocol::Imap | HealthProtocol::ImapStarttls => {
            let greeting = line(&mut io, &mut budget, 4096).await?;
            ensure!(
                greeting.len() >= 4
                    && greeting[..4].eq_ignore_ascii_case(b"* OK")
                    && (greeting.len() == 4 || greeting[4] == b' '),
                "invalid IMAP greeting"
            );
            if policy.protocol == HealthProtocol::Imap {
                return Ok(true);
            }
            send(&mut io, b"a001 CAPABILITY\r\n").await?;
            ensure!(
                imap(&mut io, b"a001", &mut budget).await?,
                "IMAP STARTTLS capability absent"
            );
            send(&mut io, b"a002 STARTTLS\r\n").await?;
            imap(&mut io, b"a002", &mut budget).await?;
            io = upgrade(io, config, tls).await?;
            send(&mut io, b"a003 NOOP\r\n").await?;
            imap(&mut io, b"a003", &mut budget).await?;
        }
        HealthProtocol::Pop3 | HealthProtocol::Pop3Starttls => {
            pop3(&mut io, &mut budget).await?;
            if policy.protocol == HealthProtocol::Pop3 {
                return Ok(true);
            }
            send(&mut io, b"CAPA\r\n").await?;
            pop3(&mut io, &mut budget).await?;
            let mut stls = false;
            let mut ended = false;
            for _ in 0..64 {
                let value = line(&mut io, &mut budget, 4096).await?;
                if value == b"." {
                    ended = true;
                    break;
                }
                stls |= value.eq_ignore_ascii_case(b"STLS");
            }
            ensure!(stls && ended, "POP3 STLS capability absent");
            send(&mut io, b"STLS\r\n").await?;
            pop3(&mut io, &mut budget).await?;
            io = upgrade(io, config, tls).await?;
            send(&mut io, b"NOOP\r\n").await?;
            pop3(&mut io, &mut budget).await?;
            send(&mut io, b"QUIT\r\n").await?;
            pop3(&mut io, &mut budget).await?;
        }
        _ => anyhow::bail!("not a mail health protocol"),
    }
    Ok(true)
}
