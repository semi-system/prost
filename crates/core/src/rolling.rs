use anyhow::{ensure, Context, Result};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
pub(crate) struct Generation {
    pub id: u64,
    pub rolling: bool,
    pub workers: Option<Arc<crate::workers::Workers>>,
    pub snapshot: arc_swap::ArcSwapOption<crate::Snapshot>,
    pub accepting: CancellationToken,
    pub drain: CancellationToken,
    pub force: CancellationToken,
    pub finished: CancellationToken,
    pub accepts: TaskTracker,
    pub tasks: TaskTracker,
    pub connections: AtomicU64,
    draining: Mutex<Option<(Instant, Duration)>>,
}
impl Generation {
    pub fn new(
        id: u64,
        rolling: bool,
        workers: Option<Arc<crate::workers::Workers>>,
        snapshot: Arc<crate::Snapshot>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            rolling,
            workers,
            snapshot: arc_swap::ArcSwapOption::from(rolling.then_some(snapshot)),
            accepting: CancellationToken::new(),
            drain: CancellationToken::new(),
            force: CancellationToken::new(),
            finished: CancellationToken::new(),
            accepts: TaskTracker::new(),
            tasks: TaskTracker::new(),
            connections: AtomicU64::new(0),
            draining: Mutex::new(None),
        })
    }
    pub fn begin_drain(&self, seconds: u64) {
        *self.draining.lock().unwrap() = Some((Instant::now(), Duration::from_secs(seconds)));
        self.drain.cancel();
    }
    pub fn remaining(&self) -> Duration {
        self.draining
            .lock()
            .unwrap()
            .as_ref()
            .map(|(start, budget)| budget.saturating_sub(start.elapsed()))
            .unwrap_or_default()
    }
    fn status(&self) -> serde_json::Value {
        serde_json::json!({"generation":self.id,"connections":self.connections.load(Ordering::Relaxed),
            "tasks":self.tasks.len(),"draining":self.drain.is_cancelled(),
            "drain_remaining_ms":self.drain.is_cancelled().then(||self.remaining().as_millis() as u64),
            "forced":self.force.is_cancelled(),"finished":self.finished.is_cancelled()})
    }
}

impl crate::RunningProxy {
    pub(crate) fn worker_status(&self) -> serde_json::Value {
        let active = self.workers.lock().unwrap().clone();
        let retiring = self
            .retiring
            .lock()
            .unwrap()
            .clone()
            .filter(|g| !g.finished.is_cancelled());
        let counters = &self.observed.worker_retirements;
        serde_json::json!({"active":active.status(),"retiring":retiring.as_ref().map(|g|g.status()),
            "retired_total":counters[0].load(Ordering::Relaxed),
            "forced_total":counters[1].load(Ordering::Relaxed),
            "forced_connections_total":counters[2].load(Ordering::Relaxed)})
    }

    /// Event-driven barrier for worker generations older than a committed config.
    /// Waiting never holds the transaction coordinator or changes configuration.
    pub async fn wait_for_workers(
        &self,
        generation: u64,
        timeout_ms: u64,
    ) -> Result<serde_json::Value> {
        ensure!(
            (1..=300_000).contains(&timeout_ms),
            "worker wait timeout must be 1..300000 ms"
        );
        tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            let retiring = {
                let _guard = self.coordinator.lock().await;
                ensure!(
                    generation <= self.snapshot().config.generation,
                    "generation_not_committed"
                );
                self.retiring
                    .lock()
                    .unwrap()
                    .clone()
                    .filter(|g| g.id < generation)
            };
            if let Some(retiring) = retiring {
                tokio::select! {
                    _ = self.shutdown.cancelled() => anyhow::bail!("proxy shutting down"),
                    _ = retiring.finished.cancelled() => {},
                }
            }
            Ok(
                serde_json::json!({"generation":self.snapshot().config.generation,
                "target_generation":generation,"drained":true}),
            )
        })
        .await
        .context("worker_drain_timeout: waiting does not roll back configuration; inspect status")?
    }
}
