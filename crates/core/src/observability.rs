use crate::RunningProxy;
use prometheus::{
    core::{Collector, Desc},
    proto::{Counter, Gauge, LabelPair, Metric, MetricFamily, MetricType},
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Weak,
    },
    time::Instant,
};

#[derive(Default)]
#[repr(align(64))]
struct Slot {
    selected: AtomicU64,
    responses: [AtomicU64; 5],
    errors: AtomicU64,
}
pub(crate) struct Endpoint {
    pub tcp_retries: AtomicU64,
    slots: Box<[Slot]>,
    single_writer: bool,
    pub health: Arc<Health>,
}
impl Endpoint {
    pub fn new(workers: usize, single_writer: bool) -> Self {
        Self {
            tcp_retries: AtomicU64::new(0),
            slots: (0..workers).map(|_| Slot::default()).collect(),
            single_writer,
            health: Arc::new(Health::default()),
        }
    }
    fn slot(&self) -> &Slot {
        let index = prost_http::worker_slot();
        &self.slots[if index < self.slots.len() {
            index
        } else {
            index % self.slots.len()
        }]
    }
    #[inline]
    fn increment(&self, counter: &AtomicU64) {
        // Hot worker-local runtimes have exactly one writer per slot. Rolling
        // generations overlap and share endpoint stats, so they use atomic RMW.
        if self.single_writer {
            counter.store(
                counter.load(Ordering::Relaxed).wrapping_add(1),
                Ordering::Relaxed,
            );
        } else {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
    #[inline]
    pub fn selected(&self) {
        self.increment(&self.slot().selected);
    }
    #[inline]
    pub fn response(&self, status: u16) {
        if (100..600).contains(&status) {
            self.increment(&self.slot().responses[(status / 100 - 1) as usize]);
        }
    }
    pub fn error(&self) {
        self.increment(&self.slot().errors);
    }
}
#[derive(Default)]
pub(crate) struct Health {
    pub probes: [AtomicU64; 2],
    pub failures: AtomicU64,
    pub ejections: AtomicU64,
}
pub(crate) struct Upstream {
    pub rejected: [AtomicU64; 2], // unavailable, capacity
    pub pool: Arc<prost_http::PoolStats>,
}
impl Upstream {
    pub fn new(workers: usize) -> Self {
        Self {
            rejected: Default::default(),
            pool: Arc::new(prost_http::PoolStats::new(workers)),
        }
    }
}
impl Default for Upstream {
    fn default() -> Self {
        Self::new(1)
    }
}
#[derive(Default)]
pub(crate) struct Runtime {
    pub h2_write_timeouts: Arc<AtomicU64>,
    pub tls: [AtomicU64; 5], // success, error, timeout, rejected, cancelled
    pub transactions: [[AtomicU64; 5]; 4], // apply, patch, validate, TLS reload
    pub worker_retirements: [AtomicU64; 3], // retired, forced generations, forced connections
}
pub(crate) struct Handshake<'a> {
    runtime: &'a Runtime,
    histogram: &'a prometheus::Histogram,
    started: Instant,
    result: usize,
}
impl<'a> Handshake<'a> {
    pub fn new(runtime: &'a Runtime, histogram: &'a prometheus::Histogram) -> Self {
        Self {
            runtime,
            histogram,
            started: Instant::now(),
            result: 4,
        }
    }
    pub fn finish(&mut self, result: usize) {
        self.result = result;
    }
}
impl Drop for Handshake<'_> {
    fn drop(&mut self) {
        self.runtime.tls[self.result].fetch_add(1, Ordering::Relaxed);
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}
pub(crate) struct Transaction<'a> {
    runtime: &'a Runtime,
    histogram: &'a prometheus::Histogram,
    started: Instant,
    operation: usize,
    result: usize,
}
impl<'a> Transaction<'a> {
    pub fn new(
        runtime: &'a Runtime,
        histogram: &'a prometheus::Histogram,
        operation: usize,
    ) -> Self {
        Self {
            runtime,
            histogram,
            started: Instant::now(),
            operation,
            result: 4,
        }
    }
    pub fn finish<T>(&mut self, result: &anyhow::Result<T>, durable: bool) {
        self.result = match result {
            Ok(_) => {
                if durable {
                    0
                } else {
                    1
                }
            }
            Err(e) if e.to_string() == "generation_conflict" => 2,
            Err(_) => 3,
        };
    }
}
impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        self.runtime.transactions[self.operation][self.result].fetch_add(1, Ordering::Relaxed);
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}

const ENDPOINT: &[&str] = &["upstream", "endpoint"];
const CLASS: &[&str] = &["upstream", "endpoint", "class"];
const UPSTREAM_REASON: &[&str] = &["upstream", "reason"];
type Definition = (
    &'static str,
    &'static str,
    MetricType,
    &'static [&'static str],
);
const DEFINITIONS: &[Definition] = &[
    (
        "prost_tcp_connect_retries_total",
        "Additional TCP dial attempts before any upstream bytes",
        MetricType::COUNTER,
        ENDPOINT,
    ),
    (
        "prost_proxy_connections_active",
        "PROXY v2 TCP sessions and scoped HTTP sockets including connects and idle",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_proxy_connections_limit",
        "Global PROXY v2 transport budget",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_http_upstream_connections_untracked",
        "Open transports created without activity tracking",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_h2_stream_write_timeouts_total",
        "H2 streams dropped after write progress deadline",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_records_filtered_total",
        "Records or requests excluded by status/path policy",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_http_upstream_pool_state_tracking",
        "Transport activity tracking enabled",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connections_busy",
        "Open transports assigned to active requests/bodies/tunnels",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connections_idle",
        "Open transports without an active assignment",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_cache_hits_total",
        "Fresh shared cache hits",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_cache_misses_total",
        "Eligible cache lookups without fresh object",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_cache_evictions_total",
        "FIFO entries evicted for budgets",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_cache_bytes",
        "Reserved cache payload/header cost including captures",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_cache_entries",
        "Completed cache entries",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_retry_attempts_total",
        "Additional bodyless GET/HEAD attempts",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_plugin_calls_total",
        "Completed or cancelled plugin calls by type/hook/result",
        MetricType::COUNTER,
        &["plugin", "hook", "result"],
    ),
    (
        "prost_plugin_seconds_total",
        "Plugin invocation duration including WASM scheduling",
        MetricType::COUNTER,
        &["plugin", "hook"],
    ),
    (
        "prost_access_log_enabled",
        "Access log sink configured",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_access_log_writer_alive",
        "Access log writer thread alive",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_access_log_records_accepted_total",
        "Log records accepted into the bounded queue",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_records_processed_total",
        "Log records consumed by writer, not a durability guarantee",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_records_sampled_out_total",
        "Requests or sessions skipped by sampling",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_records_pending",
        "Records awaiting writer dequeue including transient enqueue attempts",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_access_log_observations_active",
        "Request or session log tickets still alive",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_access_log_records_dropped_total",
        "Dropped or IO-uncertain log records",
        MetricType::COUNTER,
        &["reason"],
    ),
    (
        "prost_access_log_io_errors_total",
        "Writer IO failures",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_reopens_total",
        "Completed explicit log reopens",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_access_log_flushes_total",
        "Successful buffer flushes, not fsync",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_http_upstream_connect_attempts_total",
        "Started connector attempts",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connections_open",
        "Live upstream transport objects including upgrades and draining pools",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connections_opened_total",
        "Successful upstream transports",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connections_closed_total",
        "Dropped upstream transports",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connecting",
        "Pending DNS TCP TLS attempts",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connect_total",
        "Connector attempts by result",
        MetricType::COUNTER,
        &["upstream", "result"],
    ),
    (
        "prost_http_upstream_connect_seconds_total",
        "Total DNS TCP TLS attempt duration including cancellations",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_tls_connect_attempts_total",
        "TLS transport attempts including DNS and TCP",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_tls_connect_success_total",
        "Established TLS transports",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_tls_connect_seconds_total",
        "TLS attempt duration including DNS TCP and cancellations",
        MetricType::COUNTER,
        &["upstream"],
    ),
    (
        "prost_http_upstream_reuse_tracking",
        "Whether responses are counted on transport reuse",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_http_upstream_connection_responses_total",
        "Completed response headers on first or reused transport, not pool hit miss",
        MetricType::COUNTER,
        &["upstream", "usage"],
    ),
    (
        "prost_upstream_selections_total",
        "Endpoint admissions, including failed connects",
        MetricType::COUNTER,
        ENDPOINT,
    ),
    (
        "prost_upstream_responses_total",
        "Original upstream HTTP response headers by status class",
        MetricType::COUNTER,
        CLASS,
    ),
    (
        "prost_upstream_forward_errors_total",
        "Connect or forward errors before response outcome",
        MetricType::COUNTER,
        ENDPOINT,
    ),
    (
        "prost_upstream_enabled",
        "Endpoint administrative enabled state",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_backup",
        "Endpoint backup tier",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_healthy",
        "Endpoint eligibility from health observations",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_active",
        "Active requests or sessions when load tracking is enabled",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_max_active",
        "Configured active limit; zero means unlimited",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_active_tracking",
        "Whether active load is tracked",
        MetricType::GAUGE,
        &["upstream"],
    ),
    (
        "prost_upstream_rejections_total",
        "New admissions rejected by backend availability or capacity",
        MetricType::COUNTER,
        UPSTREAM_REASON,
    ),
    (
        "prost_upstream_health_probes_total",
        "Completed active health probes",
        MetricType::COUNTER,
        &["upstream", "endpoint", "result"],
    ),
    (
        "prost_upstream_passive_failures_total",
        "Recorded passive failure events",
        MetricType::COUNTER,
        ENDPOINT,
    ),
    (
        "prost_upstream_passive_ejections_total",
        "Passive isolation events",
        MetricType::COUNTER,
        ENDPOINT,
    ),
    (
        "prost_upstream_passive_ejected",
        "Endpoint waiting for passive recovery",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_passive_recent_failures",
        "Failures currently retained in the rolling window",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_upstream_passive_cooldown_seconds",
        "Remaining passive isolation minimum",
        MetricType::GAUGE,
        ENDPOINT,
    ),
    (
        "prost_tls_handshakes_total",
        "Downstream TLS handshake outcomes including admission rejections",
        MetricType::COUNTER,
        &["result"],
    ),
    (
        "prost_tls_certificate_not_after_seconds",
        "Active frontend certificate expiry as Unix seconds",
        MetricType::GAUGE,
        &["listener", "certificate"],
    ),
    (
        "prost_tls_certificate_seconds_remaining",
        "Seconds until active frontend certificate expiry",
        MetricType::GAUGE,
        &["listener", "certificate"],
    ),
    (
        "prost_config_transactions_total",
        "Runtime API operations including detached completions",
        MetricType::COUNTER,
        &["operation", "result"],
    ),
    (
        "prost_config_generation",
        "Currently published generation",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_worker_generations_live",
        "Active and retiring worker/listener generations",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_worker_retiring_connections",
        "Connections retained by the draining generation",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_worker_drain_remaining_seconds",
        "Remaining drain deadline for the retiring generation",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_worker_generations_retired_total",
        "Worker generations fully stopped and joined",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_worker_generations_forced_total",
        "Worker generations that reached the drain deadline",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_worker_connections_forced_total",
        "Connections present when a generation reached its drain deadline",
        MetricType::COUNTER,
        &[],
    ),
    (
        "prost_live_generations",
        "Config snapshots still referenced",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_snapshot_http_client_pools",
        "Distinct client pool groups referenced by live snapshots",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_snapshot_http_worker_clients",
        "Worker clients in snapshot-referenced pool groups",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_requests_active",
        "Global HTTP permits in use, including streaming bodies",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_tls_handshakes_active",
        "TLS handshake permits in use",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_health_probes_active",
        "Global active probe permits in use",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_tasks_tracked",
        "Tasks registered in the proxy task tracker",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_process_resident_memory_bytes",
        "Process VmRSS from Linux procfs",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_process_virtual_memory_bytes",
        "Process VmSize from Linux procfs",
        MetricType::GAUGE,
        &[],
    ),
    (
        "prost_process_open_fds",
        "Open file descriptors including the procfs directory read",
        MetricType::GAUGE,
        &[],
    ),
];
pub(crate) struct Exporter {
    proxy: Weak<RunningProxy>,
    descriptions: Vec<Desc>,
}
impl Exporter {
    pub fn new(proxy: Weak<RunningProxy>) -> prometheus::Result<Self> {
        let descriptions = DEFINITIONS
            .iter()
            .map(|(name, help, _, labels)| {
                Desc::new(
                    (*name).into(),
                    (*help).into(),
                    labels.iter().map(|s| (*s).into()).collect(),
                    HashMap::new(),
                )
            })
            .collect::<prometheus::Result<_>>()?;
        Ok(Self {
            proxy,
            descriptions,
        })
    }
}
impl Collector for Exporter {
    fn desc(&self) -> Vec<&Desc> {
        self.descriptions.iter().collect()
    }
    fn collect(&self) -> Vec<MetricFamily> {
        let Some(p) = self.proxy.upgrade() else {
            return Vec::new();
        };
        let snapshot = p.snapshot();
        let mut families: Vec<_> = DEFINITIONS
            .iter()
            .map(|(name, help, kind, _)| {
                let mut f = MetricFamily::default();
                f.set_name((*name).into());
                f.set_help((*help).into());
                f.set_field_type(*kind);
                f
            })
            .collect();
        let mut emit = |name: &str, values: &[&str], value: f64| {
            let index = DEFINITIONS.iter().position(|d| d.0 == name).unwrap();
            let mut metric = Metric::default();
            // Infer Vec or RepeatedField when an embedding application enables
            // Prometheus protobuf through Cargo feature unification.
            metric.set_label(
                DEFINITIONS[index]
                    .3
                    .iter()
                    .zip(values)
                    .map(|(key, val)| {
                        let mut pair = LabelPair::default();
                        pair.set_name((*key).into());
                        pair.set_value((*val).into());
                        pair
                    })
                    .collect(),
            );
            if DEFINITIONS[index].2 == MetricType::COUNTER {
                let mut c = Counter::default();
                c.set_value(value);
                metric.set_counter(c);
            } else {
                let mut g = Gauge::default();
                g.set_value(value);
                metric.set_gauge(g);
            }
            families[index].mut_metric().push(metric);
        };
        for (name, u) in &snapshot.upstreams {
            let pool = u.observed_group.pool.snapshot();
            emit(
                "prost_http_upstream_pool_state_tracking",
                &[name],
                f64::from(u.config.observe_pool_state),
            );
            if u.config.observe_pool_state {
                emit(
                    "prost_http_upstream_connections_busy",
                    &[name],
                    pool.busy as f64,
                );
                emit(
                    "prost_http_upstream_connections_idle",
                    &[name],
                    pool.tracked_open.saturating_sub(pool.busy) as f64,
                );
                emit(
                    "prost_http_upstream_connections_untracked",
                    &[name],
                    pool.open.saturating_sub(pool.tracked_open) as f64,
                );
            }
            if let Some(http) = &u.http {
                let stats = http.stats();
                for (key, value) in [
                    ("prost_http_cache_hits_total", stats.cache_hits),
                    ("prost_http_cache_misses_total", stats.cache_misses),
                    ("prost_http_cache_evictions_total", stats.cache_evictions),
                    ("prost_http_cache_bytes", stats.cache_bytes as u64),
                    ("prost_http_cache_entries", stats.cache_entries as u64),
                    ("prost_http_retry_attempts_total", stats.retries),
                ] {
                    emit(key, &[name], value as f64);
                }
            }
            for (metric, value) in [
                ("prost_http_upstream_connect_attempts_total", pool.attempts),
                ("prost_http_upstream_connections_open", pool.open),
                ("prost_http_upstream_connections_opened_total", pool.opened),
                ("prost_http_upstream_connections_closed_total", pool.closed),
                ("prost_http_upstream_connecting", pool.connecting),
                (
                    "prost_http_upstream_tls_connect_attempts_total",
                    pool.tls_attempts,
                ),
                (
                    "prost_http_upstream_tls_connect_success_total",
                    pool.tls_success,
                ),
            ] {
                emit(metric, &[name], value as f64);
            }
            for (result, value) in [
                ("success", pool.succeeded),
                ("error", pool.errors),
                ("timeout", pool.timeouts),
                ("cancelled", pool.cancelled),
            ] {
                emit(
                    "prost_http_upstream_connect_total",
                    &[name, result],
                    value as f64,
                );
            }
            emit(
                "prost_http_upstream_connect_seconds_total",
                &[name],
                pool.connect_micros as f64 / 1_000_000.,
            );
            emit(
                "prost_http_upstream_tls_connect_seconds_total",
                &[name],
                pool.tls_micros as f64 / 1_000_000.,
            );
            emit(
                "prost_http_upstream_reuse_tracking",
                &[name],
                f64::from(u.config.observe_connection_reuse),
            );
            for (usage, value) in [
                ("first", pool.first_responses),
                ("reused", pool.reused_responses),
            ] {
                emit(
                    "prost_http_upstream_connection_responses_total",
                    &[name, usage],
                    value as f64,
                );
            }
            emit(
                "prost_upstream_active_tracking",
                &[name],
                f64::from(u.balancer.active(0).is_some()),
            );
            for (i, e) in u.config.endpoints.iter().enumerate() {
                let labels = [name.as_str(), e.id.as_str()];
                let stats = &u.observed[i];
                emit(
                    "prost_tcp_connect_retries_total",
                    &labels,
                    stats.tcp_retries.load(Ordering::Relaxed) as f64,
                );
                emit(
                    "prost_upstream_selections_total",
                    &labels,
                    stats
                        .slots
                        .iter()
                        .map(|s| s.selected.load(Ordering::Relaxed))
                        .sum::<u64>() as f64,
                );
                emit(
                    "prost_upstream_forward_errors_total",
                    &labels,
                    stats
                        .slots
                        .iter()
                        .map(|s| s.errors.load(Ordering::Relaxed))
                        .sum::<u64>() as f64,
                );
                for class in 0..5 {
                    let class_label = ["1xx", "2xx", "3xx", "4xx", "5xx"][class];
                    emit(
                        "prost_upstream_responses_total",
                        &[name, &e.id, class_label],
                        stats
                            .slots
                            .iter()
                            .map(|s| s.responses[class].load(Ordering::Relaxed))
                            .sum::<u64>() as f64,
                    );
                }
                emit("prost_upstream_enabled", &labels, f64::from(e.enabled));
                emit("prost_upstream_backup", &labels, f64::from(e.backup));
                emit(
                    "prost_upstream_healthy",
                    &labels,
                    f64::from(u.health[i].healthy.load(Ordering::Relaxed)),
                );
                emit("prost_upstream_max_active", &labels, e.max_active as f64);
                if let Some(active) = u.balancer.active(i) {
                    emit("prost_upstream_active", &labels, active as f64);
                }
                let health = &u.health[i].observed;
                for (j, result) in ["success", "error"].iter().enumerate() {
                    emit(
                        "prost_upstream_health_probes_total",
                        &[name, &e.id, result],
                        health.probes[j].load(Ordering::Relaxed) as f64,
                    );
                }
                emit(
                    "prost_upstream_passive_failures_total",
                    &labels,
                    health.failures.load(Ordering::Relaxed) as f64,
                );
                emit(
                    "prost_upstream_passive_ejections_total",
                    &labels,
                    health.ejections.load(Ordering::Relaxed) as f64,
                );
                if let Some(passive) = u.health[i].passive_status() {
                    emit(
                        "prost_upstream_passive_ejected",
                        &labels,
                        f64::from(passive["ejected"].as_bool().unwrap_or(false)),
                    );
                    emit(
                        "prost_upstream_passive_recent_failures",
                        &labels,
                        passive["recent_failures"].as_u64().unwrap_or(0) as f64,
                    );
                    emit(
                        "prost_upstream_passive_cooldown_seconds",
                        &labels,
                        passive["cooldown_remaining_ms"].as_u64().unwrap_or(0) as f64 / 1000.,
                    );
                }
            }
            for (i, reason) in ["unavailable", "capacity"].iter().enumerate() {
                emit(
                    "prost_upstream_rejections_total",
                    &[name, reason],
                    u.observed_group.rejected[i].load(Ordering::Relaxed) as f64,
                );
            }
        }
        for (i, result) in ["success", "error", "timeout", "rejected", "cancelled"]
            .iter()
            .enumerate()
        {
            emit(
                "prost_tls_handshakes_total",
                &[result],
                p.observed.tls[i].load(Ordering::Relaxed) as f64,
            );
        }
        emit(
            "prost_access_log_enabled",
            &[],
            f64::from(p.access_log.is_some()),
        );
        emit(
            "prost_h2_stream_write_timeouts_total",
            &[],
            p.observed.h2_write_timeouts.load(Ordering::Relaxed) as f64,
        );
        emit(
            "prost_proxy_connections_limit",
            &[],
            snapshot.config.runtime.max_proxy_connections as f64,
        );
        emit(
            "prost_proxy_connections_active",
            &[],
            snapshot
                .config
                .runtime
                .max_proxy_connections
                .saturating_sub(p.proxy_connections.available_permits()) as f64,
        );
        if snapshot.config.runtime.observe_plugins {
            let names: HashSet<_> = snapshot
                .config
                .listeners
                .iter()
                .flat_map(|l| {
                    l.plugins
                        .iter()
                        .chain(l.routes.iter().flat_map(|r| &r.plugins))
                })
                .map(|p| p.name.as_str())
                .collect();
            for (name, stats) in p.registry.plugin_metrics() {
                if !names.contains(name.as_str()) {
                    continue;
                }
                for (h, hook) in prost_plugin::metrics::HOOKS.iter().enumerate() {
                    emit(
                        "prost_plugin_seconds_total",
                        &[&name, hook],
                        stats.nanos[h] as f64 / 1_000_000_000.,
                    );
                    for (r, result) in ["success", "error", "rejected", "cancelled"]
                        .iter()
                        .enumerate()
                    {
                        emit(
                            "prost_plugin_calls_total",
                            &[&name, hook, result],
                            stats.results[h][r] as f64,
                        );
                    }
                }
            }
        }
        if let Some(log) = &p.access_log {
            let c = &log.counters;
            for (name, value) in [
                ("prost_access_log_records_filtered_total", &c.filtered),
                ("prost_access_log_writer_alive", &c.alive),
                ("prost_access_log_records_accepted_total", &c.accepted),
                ("prost_access_log_records_processed_total", &c.processed),
                ("prost_access_log_records_sampled_out_total", &c.sampled_out),
                ("prost_access_log_records_pending", &c.pending),
                ("prost_access_log_observations_active", &c.active),
                ("prost_access_log_io_errors_total", &c.io_errors),
                ("prost_access_log_reopens_total", &c.reopens),
                ("prost_access_log_flushes_total", &c.flushes),
            ] {
                emit(name, &[], value.load(Ordering::Relaxed) as f64);
            }
            for (i, reason) in ["queue_full", "io_uncertain", "closed"].iter().enumerate() {
                emit(
                    "prost_access_log_records_dropped_total",
                    &[reason],
                    c.dropped[i].load(Ordering::Relaxed) as f64,
                );
            }
        }
        for (i, operation) in ["apply", "endpoint_patch", "validate", "tls_reload"]
            .iter()
            .enumerate()
        {
            for (j, result) in ["success", "not_durable", "conflict", "error", "cancelled"]
                .iter()
                .enumerate()
            {
                emit(
                    "prost_config_transactions_total",
                    &[operation, result],
                    p.observed.transactions[i][j].load(Ordering::Relaxed) as f64,
                );
            }
        }
        let live: Vec<_> = {
            let mut generations = p.generations.lock().unwrap();
            generations.retain(|g| g.strong_count() > 0);
            generations.iter().filter_map(Weak::upgrade).collect()
        };
        let mut pools = HashSet::new();
        let mut clients = 0;
        for generation in &live {
            for u in generation.upstreams.values() {
                if pools.insert(Arc::as_ptr(&u.client) as usize) {
                    clients += u.client.worker_count();
                }
            }
        }
        emit(
            "prost_config_generation",
            &[],
            snapshot.config.generation as f64,
        );
        emit("prost_live_generations", &[], live.len() as f64);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as f64;
        for (id, listener) in &snapshot.listeners {
            for (certificate, expires) in &listener.certificate_expirations {
                emit(
                    "prost_tls_certificate_not_after_seconds",
                    &[id, certificate],
                    *expires as f64,
                );
                emit(
                    "prost_tls_certificate_seconds_remaining",
                    &[id, certificate],
                    *expires as f64 - now,
                );
            }
        }
        let retiring = p
            .retiring
            .lock()
            .unwrap()
            .clone()
            .filter(|g| !g.finished.is_cancelled());
        emit(
            "prost_worker_generations_live",
            &[],
            if retiring.is_some() { 2. } else { 1. },
        );
        emit(
            "prost_worker_retiring_connections",
            &[],
            retiring
                .as_ref()
                .map_or(0, |g| g.connections.load(Ordering::Relaxed)) as f64,
        );
        emit(
            "prost_worker_drain_remaining_seconds",
            &[],
            retiring
                .as_ref()
                .map_or(0., |g| g.remaining().as_secs_f64()),
        );
        for (index, name) in [
            "prost_worker_generations_retired_total",
            "prost_worker_generations_forced_total",
            "prost_worker_connections_forced_total",
        ]
        .iter()
        .enumerate()
        {
            emit(
                name,
                &[],
                p.observed.worker_retirements[index].load(Ordering::Relaxed) as f64,
            );
        }
        emit("prost_snapshot_http_client_pools", &[], pools.len() as f64);
        emit("prost_snapshot_http_worker_clients", &[], clients as f64);
        emit(
            "prost_requests_active",
            &[],
            snapshot
                .config
                .runtime
                .max_requests
                .saturating_sub(p.requests.available_permits()) as f64,
        );
        emit(
            "prost_tls_handshakes_active",
            &[],
            256usize.saturating_sub(p.handshakes.available_permits()) as f64,
        );
        emit(
            "prost_health_probes_active",
            &[],
            32usize.saturating_sub(p.health_capacity.available_permits()) as f64,
        );
        emit("prost_tasks_tracked", &[], p.tasks.len() as f64);
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for (field, metric) in [
                ("VmRSS:", "prost_process_resident_memory_bytes"),
                ("VmSize:", "prost_process_virtual_memory_bytes"),
            ] {
                if let Some(n) = status
                    .lines()
                    .find(|line| line.starts_with(field))
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    emit(metric, &[], (n * 1024) as f64);
                }
            }
        }
        if let Ok(fds) = std::fs::read_dir("/proc/self/fd") {
            emit("prost_process_open_fds", &[], fds.count() as f64);
        }
        families.retain(|f| !f.get_metric().is_empty());
        families
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn rolling_generations_share_counters_without_losing_updates() -> anyhow::Result<()> {
        let mut config = prost_config::Config::parse(
            "schema_version=1\n[[upstreams.main.endpoints]]\nid='one'\naddress='127.0.0.1:19000'\n[[listeners]]\nid='web'\naddress='127.0.0.1:18000'\nprotocol='http'\nupstream='main'\n",
        )?;
        assert_eq!(
            config.runtime.reload_mode,
            prost_config::ReloadMode::Rolling
        );
        let registry = prost_plugin::PluginRegistry::new()?;
        let first = crate::prepare(config.clone(), &registry, None, 1)?;
        config.generation = 1;
        let next = crate::prepare(config, &registry, Some(&first), 1)?;
        let old = &first.upstreams["main"].observed[0];
        let new = &next.upstreams["main"].observed[0];
        assert!(Arc::ptr_eq(old, new));
        assert!(!old.single_writer);
        std::thread::scope(|scope| {
            for index in 0..8 {
                let endpoint = if index % 2 == 0 { old } else { new };
                scope.spawn(move || {
                    prost_http::set_worker_slot(0);
                    for _ in 0..20000 {
                        endpoint.selected();
                        endpoint.response(200);
                        endpoint.error();
                    }
                });
            }
        });
        let slot = &new.slots[0];
        assert_eq!(slot.selected.load(Ordering::Relaxed), 160000);
        assert_eq!(slot.responses[1].load(Ordering::Relaxed), 160000);
        assert_eq!(slot.errors.load(Ordering::Relaxed), 160000);
        Ok(())
    }
    #[test]
    fn worker_local_and_shared_counters_are_exact() {
        let local = Arc::new(Endpoint::new(2, true));
        std::thread::scope(|scope| {
            for slot in 0..2 {
                let local = local.clone();
                scope.spawn(move || {
                    prost_http::set_worker_slot(slot);
                    for _ in 0..10000 {
                        local.selected();
                        local.response(200);
                    }
                });
            }
        });
        assert_eq!(
            local
                .slots
                .iter()
                .map(|s| s.selected.load(Ordering::Relaxed))
                .sum::<u64>(),
            20000
        );
        assert_eq!(
            local
                .slots
                .iter()
                .map(|s| s.responses[1].load(Ordering::Relaxed))
                .sum::<u64>(),
            20000
        );
        let shared = Arc::new(Endpoint::new(1, false));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let shared = shared.clone();
                scope.spawn(move || {
                    for _ in 0..10000 {
                        shared.selected();
                        shared.response(503);
                    }
                });
            }
        });
        assert_eq!(shared.slots[0].selected.load(Ordering::Relaxed), 80000);
        assert_eq!(shared.slots[0].responses[4].load(Ordering::Relaxed), 80000);
    }
}
