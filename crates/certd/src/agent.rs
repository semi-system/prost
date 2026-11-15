use crate::{
    config::{Certificate, Config},
    http::{BoundedClient, State},
    store::{atomic, now, private_read, Bundle, Store},
};
use anyhow::{ensure, Context, Result};
use instant_acme::{
    Account, AuthorizationStatus, ChallengeType, Identifier, NewAccount, NewOrder, OrderStatus,
    RetryPolicy,
};
use prost_control::{request, Command};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

struct Challenges {
    state: Arc<State>,
    keys: Vec<(String, String)>,
}
impl Drop for Challenges {
    fn drop(&mut self) {
        let mut tokens = self.state.tokens.write().unwrap();
        for key in &self.keys {
            tokens.remove(key);
        }
    }
}

async fn account(config: &Config, store: &Store) -> Result<Account> {
    let id = format!("{:x}", Sha256::digest(config.directory_url.as_bytes()));
    let path = store.state.join(format!("account-{id}.json"));
    let builder = Account::builder_with_http(Box::new(BoundedClient::new(
        config.directory_ca.as_deref(),
    )?));
    if path.exists() {
        return Ok(builder
            .from_credentials(serde_json::from_slice(&private_read(&path, 1024 * 1024)?)?)
            .await?);
    }
    let (account, credentials) = builder
        .create(
            &NewAccount {
                contact: &[config.contact.as_str()],
                terms_of_service_agreed: config.terms_agreed,
                only_return_existing: false,
            },
            config.directory_url.clone(),
            None,
        )
        .await?;
    atomic(&path, &serde_json::to_vec(&credentials)?, 0o600)?;
    Ok(account)
}
async fn issue(
    account: &Account,
    spec: &Certificate,
    state: Arc<State>,
    budget: u64,
    config: &Config,
    store: &Store,
) -> Result<(String, String)> {
    let identifiers: Vec<_> = spec.domains.iter().cloned().map(Identifier::Dns).collect();
    let mut order = account.new_order(&NewOrder::new(&identifiers)).await?;
    let mut guard = Challenges {
        state,
        keys: Vec::new(),
    };
    let mut authorizations = order.authorizations();
    while let Some(result) = authorizations.next().await {
        let mut authorization = result?;
        match authorization.status {
            AuthorizationStatus::Valid => continue,
            AuthorizationStatus::Pending => {}
            _ => anyhow::bail!("ACME authorization is not pending or valid"),
        }
        let dns = spec.challenge == crate::config::Challenge::Dns01;
        let mut challenge = authorization
            .challenge(if dns {
                ChallengeType::Dns01
            } else {
                ChallengeType::Http01
            })
            .context("CA did not offer configured challenge")?;
        let domain = challenge.identifier().to_string().to_ascii_lowercase();
        ensure!(
            spec.domains
                .iter()
                .any(|name| name.trim_start_matches("*.") == domain.trim_start_matches("*.")),
            "CA requested an unexpected identifier"
        );
        if dns {
            crate::dns01::present(
                config,
                spec,
                store,
                crate::dns01::Record {
                    name: format!("_acme-challenge.{}", domain.trim_start_matches("*.")),
                    value: challenge.key_authorization().dns_value(),
                },
            )
            .await?;
            challenge.set_ready().await?;
            continue;
        }
        let token = challenge.token.clone();
        ensure!(
            !token.is_empty()
                && token.len() <= 256
                && token
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
            "invalid HTTP-01 token"
        );
        let value = challenge.key_authorization().as_str().to_owned();
        ensure!(value.len() <= 1024, "HTTP-01 value exceeds budget");
        let key = (domain, token);
        guard
            .state
            .tokens
            .write()
            .unwrap()
            .insert(key.clone(), value);
        guard.keys.push(key);
        challenge.set_ready().await?;
    }
    let policy = RetryPolicy::new()
        .initial_delay(Duration::from_millis(250))
        .timeout(Duration::from_secs(budget));
    ensure!(
        order.poll_ready(&policy).await? == OrderStatus::Ready,
        "ACME order is not ready"
    );
    let key = order.finalize().await?;
    let chain = order.poll_certificate(&policy).await?;
    Ok((chain, key))
}

async fn publish(config: &Config, spec: &Certificate, bundle: &Bundle) -> Result<()> {
    let domains: HashSet<_> = spec.domains.iter().cloned().collect();
    for attempt in 0..4 {
        let exported = tokio::time::timeout(
            Duration::from_secs(15),
            request(&config.admin_socket, Command::Export),
        )
        .await??;
        let text = exported
            .result
            .as_ref()
            .and_then(|v| v["config"].as_str())
            .context("missing config export")?;
        let current = prost_config::Config::parse(text)?;
        let mut updates = BTreeMap::new();
        for id in &spec.listeners {
            let listener = current
                .listeners
                .iter()
                .find(|l| &l.id == id)
                .context("certificate target listener missing")?;
            let mut tls = listener
                .tls
                .clone()
                .context("certificate target is not a TLS listener")?;
            if spec.default_certificate {
                tls.certificate = bundle.certificate.clone();
                tls.private_key = bundle.private_key.clone();
            }
            let mut covered = HashSet::new();
            let mut names = Vec::new();
            for certificate in &mut tls.certificates {
                certificate.server_names.retain(|name| {
                    if domains.contains(name) {
                        covered.insert(name.clone());
                        names.push(name.clone());
                        false
                    } else {
                        true
                    }
                });
            }
            tls.certificates
                .retain(|certificate| !certificate.server_names.is_empty());
            if spec.add_sni_names {
                for name in &spec.domains {
                    if !covered.contains(name) {
                        names.push(name.clone());
                    }
                }
            }
            ensure!(
                !tls.reject_unknown_sni || !names.is_empty(),
                "certificate has no permitted SNI names on target listener"
            );
            if !names.is_empty() {
                tls.certificates.push(prost_config::NamedCertificate {
                    server_names: names,
                    certificate: bundle.certificate.clone(),
                    private_key: bundle.private_key.clone(),
                });
            }
            updates.insert(id.clone(), tls);
        }
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            request(
                &config.admin_socket,
                Command::ReloadTlsMany {
                    updates,
                    expected_generation: exported.generation,
                },
            ),
        )
        .await?;
        match result {
            Ok(response) => {
                ensure!(
                    response
                        .result
                        .as_ref()
                        .and_then(|v| v["durable"].as_bool())
                        == Some(true),
                    "certificate commit durability is uncertain; inspect daemon status"
                );
                return Ok(());
            }
            Err(error) if attempt < 3 && error.to_string().contains("generation_conflict") => {
                continue
            }
            Err(error) => return Err(error),
        }
    }
    anyhow::bail!("certificate transaction conflicts exceeded retry budget")
}
struct Entry {
    bundle: Bundle,
    pending: bool,
    next: i64,
}
pub async fn maintain(
    config: Config,
    store: Arc<Store>,
    state: Arc<State>,
    bundles: Vec<Bundle>,
    stop: CancellationToken,
) {
    // Readiness probing is not a configuration watcher. Do not order a cert
    // until the edge has bootstrapped and its administration socket is available.
    loop {
        if tokio::time::timeout(
            Duration::from_secs(2),
            request(&config.admin_socket, Command::Status),
        )
        .await
        .is_ok_and(|r| r.is_ok())
        {
            break;
        }
        tokio::select! {_=stop.cancelled()=>return,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
    }
    let credentials = Arc::new(tokio::sync::Mutex::new(None));
    let budget = Arc::new(tokio::sync::Semaphore::new(4));
    let mut tasks = tokio::task::JoinSet::new();
    let config = Arc::new(config);
    for (spec, bundle) in config.certificates.iter().cloned().zip(bundles) {
        let (config, store, state, credentials, budget, stop) = (
            config.clone(),
            store.clone(),
            state.clone(),
            credentials.clone(),
            budget.clone(),
            stop.clone(),
        );
        tasks.spawn(async move {
            let mut entry = Entry {
                pending: !bundle.bootstrap && bundle.not_after > now() + 60,
                bundle,
                next: 0,
            };
            loop {
                let delay = Duration::from_secs((entry.next - now()).max(0) as u64);
                tokio::select! { _=stop.cancelled()=>return, _=tokio::time::sleep(delay)=>{} }
                let cycle = async {
                    let _permit = budget.acquire().await.unwrap();
                    maintain_one(&config, &spec, &mut entry, &store, &state, &credentials).await;
                };
                tokio::select! { _=stop.cancelled()=>return, _=cycle=>{} }
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(error=%error,"certificate worker failed");
            stop.cancel();
        }
    }
}

async fn maintain_one(
    config: &Config,
    spec: &Certificate,
    entry: &mut Entry,
    store: &Arc<Store>,
    state: &Arc<State>,
    credentials: &tokio::sync::Mutex<Option<Account>>,
) {
    let result: Result<()> = async {
        if !entry.pending
            && (entry.bundle.bootstrap
                || entry.bundle.not_after - config.renew_before_seconds as i64 <= now())
        {
            crate::dns01::cleanup(config, spec, store).await?;
            let account = {
                let mut credentials = credentials.lock().await;
                if credentials.is_none() {
                    *credentials = Some(tokio::time::timeout(
                        Duration::from_secs(config.order_timeout_seconds), account(config, store),
                    ).await??);
                }
                credentials.as_ref().unwrap().clone()
            };
            let (chain, key) = tokio::time::timeout(
                Duration::from_secs(config.order_timeout_seconds),
                issue(&account, spec, state.clone(), config.order_timeout_seconds, config, store),
            ).await??;
            entry.bundle = store.install(spec, &chain, &key, false)?;
            entry.pending = true;
            state.issued.fetch_add(1, Ordering::Relaxed);
        }
        if entry.pending {
            publish(config, spec, &entry.bundle).await?;
            atomic(
                &store.certificates.join("publication.json"),
                &serde_json::to_vec(&serde_json::json!({
                    "certificate": spec.id, "not_after": entry.bundle.not_after, "published_at": now()
                }))?,
                if config.shared_certificate_group { 0o640 } else { 0o600 },
            )?;
            entry.pending = false;
            state.certificates.write().unwrap().insert(spec.id.clone(), crate::status::CertificateStatus {
                not_after: entry.bundle.not_after, bootstrap: entry.bundle.bootstrap,
                published: true, published_at: now(), delivery_pending: spec.delivery.len(), last_error: None,
            });
            state.published.fetch_add(1, Ordering::Relaxed);
            if let Err(error) = store.prune(&entry.bundle) {
                tracing::warn!(error=%error, "certificate history cleanup failed");
            }
            tracing::info!(certificate=%spec.id, not_after=entry.bundle.not_after, "certificate published");
        }
        // TXT cleanup must not wait for an unavailable backend's reload ACK.
        crate::dns01::cleanup(config, spec, store).await?;
        if !entry.bundle.bootstrap {
            let pending = crate::delivery::converge(store.clone(), spec, &entry.bundle).await?;
            if let Some(value) = state.certificates.write().unwrap().get_mut(&spec.id) {
                value.delivery_pending = pending;
            }
            ensure!(pending == 0, "backend certificate acknowledgments pending");
        }
        if let Some(value) = state.certificates.write().unwrap().get_mut(&spec.id) {
            value.last_error = None;
        }
        Ok(())
    }.await;
    match result {
        Ok(()) => {
            entry.next =
                (entry.bundle.not_after - config.renew_before_seconds as i64).max(now() + 60)
        }
        Err(error) => {
            state.failures.fetch_add(1, Ordering::Relaxed);
            let mut values = state.certificates.write().unwrap();
            let value = values.get_mut(&spec.id).unwrap();
            value.last_error =
                Some("certificate cycle or backend acknowledgment failed; inspect agent logs");
            entry.next = now()
                + if value.published && value.delivery_pending > 0 {
                    config.delivery_retry_seconds
                } else {
                    config.retry_seconds
                } as i64;
            tracing::warn!(certificate=%spec.id,error=%error,"certificate cycle failed; committed TLS retained");
        }
    }
}
