use super::*;
use prost_http::cache_control::CacheQuery;

impl RunningProxy {
    pub async fn cache_status(
        self: &Arc<Self>,
        upstream: Option<String>,
    ) -> Result<serde_json::Value> {
        let snapshot = self.snapshot();
        if let Some(name) = &upstream {
            ensure!(
                snapshot.upstreams.contains_key(name),
                "unknown cache upstream"
            );
        }
        tokio::task::spawn_blocking(move || {
            let caches:BTreeMap<_,_>=snapshot.upstreams.iter().filter(|(name,_)|upstream.as_ref().is_none_or(|wanted|wanted==*name)).map(|(name,u)| {
                let status=u.http.as_ref().and_then(|policy|policy.cache_status());
                (name.clone(),serde_json::json!({"enabled":status.is_some(),"status":status}))
            }).collect();
            Ok(serde_json::json!({"generation":snapshot.config.generation,"config_hash":snapshot.hash,"caches":caches}))
        }).await?
    }
    pub async fn cache_entries(
        self: &Arc<Self>,
        upstream: String,
        query: CacheQuery,
    ) -> Result<serde_json::Value> {
        query.validate()?;
        let snapshot = self.snapshot();
        let policy = snapshot
            .upstreams
            .get(&upstream)
            .context("unknown cache upstream")?
            .http
            .clone()
            .context("cache is disabled")?;
        tokio::task::spawn_blocking(move || {
            let cache=policy.cache_entries(&query)?;
            Ok(serde_json::json!({"generation":snapshot.config.generation,"config_hash":snapshot.hash,"upstream":upstream,"cache":cache}))
        }).await?
    }
    pub async fn cache_purge(
        self: &Arc<Self>,
        upstream: String,
        key: Option<String>,
        all: bool,
        expected: u64,
    ) -> Result<serde_json::Value> {
        ensure!(
            key.is_some() != all,
            "select exactly one of cache key or all"
        );
        CacheQuery {
            key: key.clone(),
            ..Default::default()
        }
        .validate()?;
        let proxy = self.clone();
        tokio::spawn(async move {
            let _guard=proxy.coordinator.lock().await;
            ensure!(!proxy.shutdown.is_cancelled(),"proxy shutting down");
            let snapshot=proxy.snapshot();
            ensure!(snapshot.config.generation==expected,"generation_conflict");
            let policy=snapshot.upstreams.get(&upstream).context("unknown cache upstream")?.http.clone().context("cache is disabled")?;
            tokio::task::spawn_blocking(move || {
                let purge=policy.purge_cache(key.as_deref())?;
                Ok(serde_json::json!({"generation":expected,"upstream":upstream,"purge":purge,"ephemeral":true,"config_changed":false}))
            }).await?
        }).await?
    }
}
