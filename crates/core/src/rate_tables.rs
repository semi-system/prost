use super::*;
use prost_plugin::rate_table::{TableKey, TableQuery};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RateTableTarget {
    pub listener: String,
    /// Declaration index, not routing priority order. None selects listener plugins.
    #[serde(default)]
    pub route: Option<usize>,
    pub plugin: usize,
}
fn selected(snapshot: &Snapshot, target: &RateTableTarget) -> Result<PreparedPlugin> {
    let listener = snapshot
        .listeners
        .get(&target.listener)
        .context("unknown rate table listener")?;
    let plugins = match target.route {
        None => &listener.plugins,
        Some(index) => listener
            .routes
            .get(index)
            .context("unknown rate table route index")?,
    };
    let plugin = plugins
        .get(target.plugin)
        .context("unknown rate table plugin index")?
        .clone();
    ensure!(
        plugin.rate_table().is_some(),
        "selected plugin has no native rate table"
    );
    Ok(plugin)
}
impl RunningProxy {
    pub async fn rate_table_list(
        self: &Arc<Self>,
        listener: Option<String>,
        limit: usize,
    ) -> Result<serde_json::Value> {
        ensure!(
            (1..=prost_plugin::rate_table::MAX_ROWS).contains(&limit),
            "rate table limit must be 1..=1024"
        );
        let snapshot = self.snapshot();
        if let Some(id) = &listener {
            ensure!(
                snapshot.listeners.contains_key(id),
                "unknown rate table listener"
            );
        }
        tokio::task::spawn_blocking(move || {
            let mut tables = Vec::new();
            let mut total = 0;
            for (id, prepared) in &snapshot.listeners {
                if listener.as_ref().is_some_and(|wanted|wanted!=id) {continue;}
                let scopes = std::iter::once((None,&prepared.plugins,&prepared.config.plugins))
                    .chain(prepared.routes.iter().zip(&prepared.config.routes).enumerate().map(|(i,(plugins,route))|(Some(i),plugins,&route.plugins)));
                for (route,plugins,configs) in scopes {
                    for (index,(plugin,config)) in plugins.iter().zip(configs).enumerate() {
                        if plugin.rate_table().is_none() {continue;}
                        total += 1;
                        if tables.len()<limit {
                            tables.push(serde_json::json!({"target":RateTableTarget {listener:id.clone(),route,plugin:index},
                                "plugin_name":config.name,"hooks":config.hooks}));
                        }
                    }
                }
            }
            Ok(serde_json::json!({"generation":snapshot.config.generation,"config_hash":snapshot.hash,
                "total_tables":total,"truncated":total>tables.len(),"tables":tables}))
        }).await?
    }
    pub async fn rate_table_show(
        self: &Arc<Self>,
        target: RateTableTarget,
        query: TableQuery,
    ) -> Result<serde_json::Value> {
        query.validate()?;
        let snapshot = self.snapshot();
        let plugin = selected(&snapshot, &target)?;
        // Administrative locks and JSON allocation never run on an I/O worker.
        tokio::task::spawn_blocking(move || {
            let table = plugin.rate_table().unwrap().inspect(&query)?;
            Ok(serde_json::json!({"generation":snapshot.config.generation,"config_hash":snapshot.hash,
                "target":target,"table":table}))
        }).await?
    }
    pub async fn rate_table_clear(
        self: &Arc<Self>,
        target: RateTableTarget,
        key: Option<TableKey>,
        all: bool,
        expected: u64,
    ) -> Result<serde_json::Value> {
        ensure!(
            key.is_some() != all,
            "select exactly one of key or all for rate table clear"
        );
        if let Some(key) = &key {
            TableQuery {
                key: Some(key.clone()),
                ..Default::default()
            }
            .validate()?;
        }
        let proxy = self.clone();
        // Once accepted, complete even if the control client disconnects. Hold
        // the config coordinator until the blocking operation has completed.
        tokio::spawn(async move {
            let _guard = proxy.coordinator.lock().await;
            ensure!(!proxy.shutdown.is_cancelled(), "proxy shutting down");
            let snapshot = proxy.snapshot();
            ensure!(snapshot.config.generation == expected, "generation_conflict");
            let plugin = selected(&snapshot, &target)?;
            tokio::task::spawn_blocking(move || {
                let affected = plugin.rate_table().unwrap().clear(key.as_ref())?;
                Ok(serde_json::json!({"generation":expected,"target":target,"affected_entries":affected,
                    "ephemeral":true,"config_changed":false}))
            }).await?
        }).await?
    }
}
