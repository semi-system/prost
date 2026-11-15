use crate::{
    config::{Certificate, Config},
    store::{atomic, private_read, Store},
};
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
pub struct Record {
    pub name: String,
    pub value: String,
}
pub async fn present(
    config: &Config,
    spec: &Certificate,
    store: &Store,
    record: Record,
) -> Result<()> {
    let provider = config.dns01.as_ref().unwrap();
    let path = store.state.join(format!("dns-{}.json", spec.id));
    let mut records: Vec<Record> = if path.exists() {
        serde_json::from_slice(&private_read(&path, 1024 * 1024)?)?
    } else {
        Vec::new()
    };
    ensure!(records.len() < 100, "DNS order record budget");
    records.push(record.clone());
    atomic(&path, &serde_json::to_vec(&records)?, 0o600)?;
    crate::webhook::post(
        &provider.present_url,
        provider.tls.as_ref(),
        provider.bearer_file.as_deref(),
        serde_json::json!({"version":1,"action":"present","record":record}),
        &format!("{}/{}", spec.id, record.value),
    )
    .await?;
    let client = prost_transport::dns_client(&prost_config::DnsPolicy {
        authoritative_ttl: true,
        nameservers: provider.nameservers.clone(),
        timeout_ms: 2000,
        ..Default::default()
    })?;
    tokio::time::timeout(
        std::time::Duration::from_secs(provider.propagation_timeout_seconds),
        async {
            loop {
                if let Ok(answer) = client.txt_lookup(format!("{}.", record.name)).await {
                    if answer.answers().iter().any(|value| match &value.data {
                        hickory_resolver::proto::rr::RData::TXT(txt) => txt
                            .txt_data
                            .iter()
                            .flat_map(|bytes| bytes.iter().copied())
                            .eq(record.value.bytes()),
                        _ => false,
                    }) {
                        return;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        },
    )
    .await?;
    Ok(())
}
pub async fn cleanup(config: &Config, spec: &Certificate, store: &Store) -> Result<()> {
    let Some(provider) = &config.dns01 else {
        return Ok(());
    };
    let path = store.state.join(format!("dns-{}.json", spec.id));
    if !path.exists() {
        return Ok(());
    }
    let mut records: Vec<Record> = serde_json::from_slice(&private_read(&path, 1024 * 1024)?)?;
    while let Some(record) = records.last() {
        crate::webhook::post(
            &provider.cleanup_url,
            provider.tls.as_ref(),
            provider.bearer_file.as_deref(),
            serde_json::json!({"version":1,"action":"cleanup","record":record}),
            &format!("{}/{}", spec.id, record.value),
        )
        .await?;
        records.pop();
        atomic(&path, &serde_json::to_vec(&records)?, 0o600)?;
    }
    Ok(())
}
