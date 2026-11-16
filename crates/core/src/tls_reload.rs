//! Certificate/policy transactions keep the current worker and established streams.
use super::*;

impl RunningProxy {
    /// Replace only one frontend TLS configuration. Source artifacts are fetched
    /// and validated before persistence; publication never starts a worker drain.
    pub async fn reload_tls(
        self: &Arc<Self>,
        listener: String,
        tls: prost_config::ServerTls,
        expected: u64,
    ) -> Result<serde_json::Value> {
        self.reload_tls_many(BTreeMap::from([(listener, tls)]), expected)
            .await
    }
    /// Atomically rotate frontend TLS on several listeners in one generation.
    pub async fn reload_tls_many(
        self: &Arc<Self>,
        updates: BTreeMap<String, prost_config::ServerTls>,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let p = self.clone();
        tokio::spawn(async move {
            let mut observation =
                observability::Transaction::new(&p.observed, &p.metrics.transaction_latency[3], 3);
            let result = p.reload_tls_inner(updates, expected).await;
            let durable = result
                .as_ref()
                .ok()
                .and_then(|v| v["durable"].as_bool())
                .unwrap_or(true);
            observation.finish(&result, durable);
            result
        })
        .await?
    }

    async fn reload_tls_inner(
        self: &Arc<Self>,
        updates: BTreeMap<String, prost_config::ServerTls>,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let _guard = self.coordinator.lock().await;
        ensure!(!self.shutdown.is_cancelled(), "proxy shutting down");
        let old = self.snapshot();
        ensure!(old.config.generation == expected, "generation_conflict");
        let mut config = old.config.clone();
        ensure!(
            !updates.is_empty() && updates.len() <= 64,
            "TLS batch must contain 1..64 listeners"
        );
        for (listener, tls) in updates {
            let target = config
                .listeners
                .iter_mut()
                .find(|l| l.id == listener)
                .context("unknown listener")?;
            ensure!(
                target.tls.is_some(),
                "TLS reload requires an existing TLS listener"
            );
            target.tls = Some(tls);
        }
        config.generation = expected.checked_add(1).context("generation overflow")?;
        let previous = old.clone();
        let registry = self.registry.clone();
        let store = self.artifacts.clone();
        let worker_count = self.worker_count;
        let new = Arc::new(
            tokio::task::spawn_blocking(move || {
                prepare_with_clients(
                    storage::stage(config, &store)?,
                    &registry,
                    Some(&previous),
                    worker_count,
                    true,
                )
            })
            .await??,
        );
        let encoded = new.config.encode()?;
        let path = self.path.clone();
        let store = self.artifacts.clone();
        let durable =
            tokio::task::spawn_blocking(move || store.persist(&path, encoded.as_bytes())).await??;
        // No fallible steps after persistence. The existing accept loops load the
        // new snapshot; existing connections retain their original snapshot.
        self.current.store(new.clone());
        let worker = self.workers.lock().unwrap().clone();
        if worker.rolling {
            worker.snapshot.store(Some(new.clone()));
        }
        self.refresh_quic_tls(&new);
        {
            let mut generations = self.generations.lock().unwrap();
            generations.retain(|g| g.strong_count() > 0);
            generations.push(Arc::downgrade(&new));
        }
        self.metrics.commits.inc();
        tracing::info!(
            generation = new.config.generation,
            durable,
            "TLS configuration committed without worker drain"
        );
        Ok(
            serde_json::json!({"generation":new.config.generation,"hash":new.hash,"durable":durable,"worker_generation":self.workers.lock().unwrap().id,"worker_drain_started":false}),
        )
    }
}
