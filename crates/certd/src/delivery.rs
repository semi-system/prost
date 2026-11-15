//! A persisted outbox is acknowledged only by a verified backend TLS handshake.
use crate::{
    config::{Certificate, Delivery, DeliveryProtocol},
    store::{atomic, private_read, Bundle, Store},
};
use anyhow::{ensure, Context, Result};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, io::BufReader, sync::Arc, time::Duration};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

#[derive(Default, Deserialize, Serialize)]
struct Outbox {
    fingerprint: String,
    acknowledged: BTreeSet<String>,
}
pub fn fingerprint(bundle: &Bundle) -> Result<String> {
    let bytes = std::fs::read(&bundle.certificate)?;
    let cert = rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice()))
        .next()
        .context("empty certificate")??;
    Ok(format!("{:x}", Sha256::digest(cert.as_ref())))
}
async fn line<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut tokio::io::BufReader<R>,
) -> Result<String> {
    let mut bytes = Vec::new();
    let count = reader.take(4097).read_until(b'\n', &mut bytes).await?;
    ensure!(
        count > 0 && count <= 4096 && bytes.ends_with(b"\r\n"),
        "invalid backend protocol line"
    );
    Ok(String::from_utf8(bytes)?)
}
async fn probe(target: &Delivery, expected: &str) -> Result<()> {
    let io = prost_transport::connect_plain_with_proxy_command(
        &target.address,
        3000,
        target
            .send_proxy_v2
            .then_some(prost_config::ProxyV2Command::Local),
        None,
        None,
    )
    .await?;
    let io = match target.protocol {
        DeliveryProtocol::Tls => io,
        DeliveryProtocol::SmtpStarttls => {
            let mut reader = tokio::io::BufReader::new(io);
            ensure!(
                line(&mut reader).await?.starts_with("220 "),
                "invalid SMTP greeting"
            );
            reader
                .get_mut()
                .write_all(b"EHLO certificate-agent\r\n")
                .await?;
            let mut ready = false;
            let mut ended = false;
            for _ in 0..32 {
                let value = line(&mut reader).await?;
                ensure!(
                    value.starts_with("250-") || value.starts_with("250 "),
                    "invalid SMTP capability reply"
                );
                ready |= value[4..].trim().eq_ignore_ascii_case("STARTTLS");
                if value.starts_with("250 ") {
                    ended = true;
                    break;
                }
            }
            ensure!(ready && ended, "backend does not offer STARTTLS");
            reader.get_mut().write_all(b"STARTTLS\r\n").await?;
            ensure!(
                line(&mut reader).await?.starts_with("220 "),
                "backend rejected STARTTLS"
            );
            reader.into_inner()
        }
        DeliveryProtocol::ImapStarttls => {
            let mut reader = tokio::io::BufReader::new(io);
            ensure!(
                line(&mut reader)
                    .await?
                    .to_ascii_uppercase()
                    .starts_with("* OK"),
                "invalid IMAP greeting"
            );
            reader.get_mut().write_all(b"a STARTTLS\r\n").await?;
            ensure!(
                line(&mut reader)
                    .await?
                    .to_ascii_uppercase()
                    .starts_with("A OK"),
                "backend rejected STARTTLS"
            );
            reader.into_inner()
        }
    };
    let tls = prost_transport::client_tls(Some(&target.tls))?;
    let name = rustls::pki_types::ServerName::try_from(target.tls.server_name.clone())?;
    let mut io = tokio_rustls::TlsConnector::from(tls)
        .connect(name, io)
        .await?;
    let certificate = io
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|c| c.first())
        .context("backend supplied no certificate")?;
    ensure!(
        format!("{:x}", Sha256::digest(certificate.as_ref())) == expected,
        "backend has not loaded the committed certificate"
    );
    io.shutdown().await?;
    Ok(())
}
async fn notify(target: &Delivery, group: &str, fingerprint: &str) -> Result<()> {
    let Some(url) = &target.notify_url else {
        return Ok(());
    };
    crate::webhook::post(url,Some(target.notify_tls.as_ref().unwrap_or(&target.tls)),target.bearer_file.as_deref(),serde_json::json!({"version":1,"certificate":group,"target":target.id,"fingerprint_sha256":fingerprint}),&format!("{group}/{fingerprint}/{}",target.id)).await
}
pub async fn converge(store: Arc<Store>, spec: &Certificate, bundle: &Bundle) -> Result<usize> {
    if spec.delivery.is_empty() {
        return Ok(0);
    }
    let expected = fingerprint(bundle)?;
    let path = store.state.join(format!("delivery-{}.json", spec.id));
    let mut outbox = if path.exists() {
        serde_json::from_slice::<Outbox>(&private_read(&path, 1024 * 1024)?)?
    } else {
        Outbox::default()
    };
    if outbox.fingerprint != expected {
        outbox = Outbox {
            fingerprint: expected.clone(),
            acknowledged: BTreeSet::new(),
        };
    }
    outbox
        .acknowledged
        .retain(|id| spec.delivery.iter().any(|target| &target.id == id));
    // Save pending work before touching a backend, then persist each ACK.
    atomic(&path, &serde_json::to_vec(&outbox)?, 0o600)?;
    let pending: Vec<_> = spec
        .delivery
        .iter()
        .filter(|target| !outbox.acknowledged.contains(&target.id))
        .cloned()
        .collect();
    let mut results = stream::iter(pending.into_iter().map(|target| {
        let expected = expected.clone();
        let group = spec.id.clone();
        async move {
            let accepted = tokio::time::timeout(Duration::from_secs(8), probe(&target, &expected))
                .await
                .is_ok_and(|r| r.is_ok());
            if !accepted {
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    notify(&target, &group, &expected),
                )
                .await;
            }
            (target.id.clone(), accepted)
        }
    }))
    .buffer_unordered(4);
    while let Some((id, accepted)) = results.next().await {
        if accepted {
            outbox.acknowledged.insert(id);
            atomic(&path, &serde_json::to_vec(&outbox)?, 0o600)?;
        }
    }
    Ok(spec.delivery.len() - outbox.acknowledged.len())
}
