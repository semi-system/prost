#![cfg(unix)]
mod access_log;
mod balance;
mod cache_control;
pub use prost_http::cache_control::{cache_key, CacheKeyInput, CacheQuery};
mod health;
mod ip_connections;
mod local_metrics;
mod observability;
mod proxy_clients;
mod rate_tables;
pub use prost_plugin::rate_table::{KeyEncoding, TableKey, TableQuery};
pub use rate_tables::RateTableTarget;
mod http3;
mod rolling;
mod route_index;
mod route_match;
mod storage;
#[cfg(test)]
mod storage_tests;
mod tls_reload;
mod udp;
mod workers;

use anyhow::{ensure, Context, Result};
use arc_swap::ArcSwap;
use http::{header, HeaderName, HeaderValue, Request, Response};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    server::conn::auto,
};
use prost_config::{Config, Listener, Protocol, Upstream};
use prost_http::{Body, ClientPool, IncomingBody};
use prost_plugin::{
    Event, FilterCall, FilterDecision, FilterHook, HeaderContext, PluginRegistry, PreparedPlugin,
};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs::File,
    io::Read,
    net::SocketAddr,
    path::PathBuf,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

static FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
static FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

pub use prost_config;
pub use prost_plugin;
pub use storage::{GcOptions, GcReport};

pub struct Metrics {
    pub udp_datagrams: prometheus::IntCounterVec,
    pub udp_dropped: prometheus::IntCounterVec,
    pub registry: prometheus::Registry,
    pub connections: prometheus::IntGauge,
    pub requests: prometheus::IntCounter,
    pub errors: prometheus::IntCounter,
    pub bytes: prometheus::IntCounter,
    pub commits: prometheus::IntCounter,
    pub latency: prometheus::Histogram,
    tls_latency: prometheus::Histogram,
    transaction_latency: [prometheus::Histogram; 4],
    gc_runs: prometheus::IntCounterVec,
    gc_files: prometheus::IntCounter,
    gc_bytes: prometheus::IntCounter,
    gc_seconds: prometheus::Histogram,
}
impl Metrics {
    fn new() -> Result<Self> {
        let registry = prometheus::Registry::new();
        let transaction_latency = prometheus::HistogramVec::new(
            prometheus::HistogramOpts::new(
                "prost_config_transaction_seconds",
                "Runtime operation duration including coordinator wait",
            ),
            &["operation"],
        )?;
        registry.register(Box::new(transaction_latency.clone()))?;
        let m = Self {
            udp_datagrams: prometheus::IntCounterVec::new(
                prometheus::Opts::new(
                    "prost_udp_datagrams_total",
                    "UDP datagrams received and returned",
                ),
                &["direction"],
            )?,
            udp_dropped: prometheus::IntCounterVec::new(
                prometheus::Opts::new(
                    "prost_udp_dropped_total",
                    "UDP datagrams rejected or discarded",
                ),
                &["reason"],
            )?,
            registry,
            gc_runs: prometheus::IntCounterVec::new(
                prometheus::Opts::new(
                    "prost_artifact_gc_runs_total",
                    "Artifact collection results",
                ),
                &["mode", "result"],
            )?,
            gc_files: prometheus::IntCounter::new(
                "prost_artifact_gc_deleted_files_total",
                "Successfully unlinked managed files",
            )?,
            gc_bytes: prometheus::IntCounter::new(
                "prost_artifact_gc_deleted_bytes_total",
                "Bytes in successfully unlinked managed files",
            )?,
            gc_seconds: prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "prost_artifact_gc_seconds",
                "Collection including coordinator wait",
            ))?,
            tls_latency: prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "prost_tls_handshake_seconds",
                "Downstream TLS handshake duration excluding rejected admission",
            ))?,
            transaction_latency: ["apply", "endpoint_patch", "validate", "tls_reload"]
                .map(|operation| transaction_latency.with_label_values(&[operation])),
            connections: prometheus::IntGauge::new(
                "prost_connections",
                "Active accepted connections",
            )?,
            requests: prometheus::IntCounter::new("prost_requests_total", "HTTP requests")?,
            errors: prometheus::IntCounter::new("prost_errors_total", "Traffic and plugin errors")?,
            bytes: prometheus::IntCounter::new(
                "prost_stream_bytes_total",
                "TCP/UDP bytes relayed",
            )?,
            commits: prometheus::IntCounter::new(
                "prost_config_commits_total",
                "Configuration commits",
            )?,
            latency: prometheus::Histogram::with_opts(prometheus::HistogramOpts::new(
                "prost_http_headers_seconds",
                "Time to proxy response headers",
            ))?,
        };
        m.registry.register(Box::new(m.connections.clone()))?;
        m.registry.register(Box::new(m.udp_datagrams.clone()))?;
        m.registry.register(Box::new(m.udp_dropped.clone()))?;
        m.registry.register(Box::new(m.requests.clone()))?;
        m.registry.register(Box::new(m.errors.clone()))?;
        m.registry.register(Box::new(m.bytes.clone()))?;
        m.registry.register(Box::new(m.commits.clone()))?;
        m.registry.register(Box::new(m.latency.clone()))?;
        m.registry.register(Box::new(m.tls_latency.clone()))?;
        m.registry.register(Box::new(m.gc_runs.clone()))?;
        m.registry.register(Box::new(m.gc_files.clone()))?;
        m.registry.register(Box::new(m.gc_bytes.clone()))?;
        m.registry.register(Box::new(m.gc_seconds.clone()))?;
        Ok(m)
    }
}
pub struct Snapshot {
    static_ram: Arc<prost_http::static_files::RamBudget>,
    pub config: Config,
    pub hash: String,
    upstreams: BTreeMap<String, Arc<PreparedUpstream>>,
    listeners: BTreeMap<String, Arc<PreparedListener>>,
    original_hash_key: bool,
    artifact_hashes: std::collections::HashSet<std::ffi::OsString>,
}
struct PreparedUpstream {
    dns: Option<Arc<prost_transport::DnsResolver>>,
    http: Option<Arc<prost_http::HttpResources>>,
    config: Upstream,
    tls: Arc<rustls::ClientConfig>,
    health_tls: Arc<rustls::ClientConfig>,
    client: Arc<ClientPool>,
    health: Vec<Arc<health::State>>,
    balancer: balance::Balancer,
    targets: Vec<prost_http::Target>,
    passive_statuses: Option<Box<[u64; 4]>>,
    observed: Vec<Arc<observability::Endpoint>>,
    observed_group: Arc<observability::Upstream>,
}
impl PreparedUpstream {
    fn selection_error(&self, error: anyhow::Error) -> anyhow::Error {
        if let Some(reason) = error.downcast_ref::<balance::Rejection>() {
            self.observed_group.rejected[match reason {
                balance::Rejection::Unavailable => 0,
                balance::Rejection::Capacity => 1,
            }]
            .fetch_add(1, Ordering::Relaxed);
        }
        error
    }
    #[inline]
    fn failed_status(&self, status: u16) -> bool {
        self.passive_statuses.as_ref().is_some_and(|mask| {
            (400..=599).contains(&status)
                && mask[(status as usize - 400) / 64] & (1 << ((status - 400) % 64)) != 0
        })
    }
}
struct PreparedListener {
    http3: Option<http3::Prepared>,
    certificate_expirations: Vec<(String, i64)>,
    connection_admission: Vec<usize>,
    ip_capacity: Arc<ip_connections::IpConnections>,
    passthrough: Option<Arc<prost_transport::ClientHelloGuard>>,
    config: Listener,
    tls: Option<Arc<rustls::ServerConfig>>,
    capacity: Arc<Semaphore>,
    plugins: Arc<Vec<PreparedPlugin>>,
    routes: Vec<Arc<Vec<PreparedPlugin>>>,
    route_order: Vec<usize>,
    route_index: route_index::RouteIndex,
    route_matches: Vec<route_match::RouteMatch>,
    default_upstream: Option<Arc<PreparedUpstream>>,
    route_upstreams: Vec<Option<Arc<PreparedUpstream>>>,
    static_files: Vec<Option<Arc<prost_http::static_files::StaticService>>>,
    has_hooks: bool,
    uri_filter: bool,
    route_uri_filters: Vec<bool>,
}
struct ConnectionInfo {
    snapshot: Option<Arc<Snapshot>>,
    addresses: prost_transport::ProxyAddresses,
    proxy_clients: proxy_clients::Clients,
    transport_peer: SocketAddr,
    headers_seen: std::sync::atomic::AtomicBool,
    first_headers: tokio::sync::Notify,
    peer: SocketAddr,
    peer_text: String,
    forwarded_for: HeaderValue,
    forwarded_proto: HeaderValue,
    _lease: ConnectionLease,
}
trait HttpMode {
    fn response(
        proxy: Arc<RunningProxy>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        request: Request<Incoming>,
        guard: bool,
    ) -> impl std::future::Future<Output = std::result::Result<Response<Body>, Infallible>>
           + Send
           + 'static;
}
struct PlainHttp;
struct LoggedHttp;
impl HttpMode for PlainHttp {
    async fn response(
        proxy: Arc<RunningProxy>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        request: Request<Incoming>,
        guard: bool,
    ) -> std::result::Result<Response<Body>, Infallible> {
        let mut response = proxy
            .handle_http(id, info, request.map(IncomingBody::from), None)
            .await;
        if guard {
            prost_http::h2_deadline::observe_response(&mut response);
        }
        Ok(response)
    }
}
impl HttpMode for LoggedHttp {
    async fn response(
        proxy: Arc<RunningProxy>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        request: Request<Incoming>,
        guard: bool,
    ) -> std::result::Result<Response<Body>, Infallible> {
        Ok(proxy
            .handle_http_logged(id, info, request.map(IncomingBody::from), guard)
            .await)
    }
}
struct ConnectionLease {
    generation: Arc<rolling::Generation>,
    _ip: Option<ip_connections::Permit>,
    audit: Option<Arc<access_log::Ticket>>,
    _global: OwnedSemaphorePermit,
    _local: OwnedSemaphorePermit,
    _plugins: Vec<OwnedSemaphorePermit>,
    active: prometheus::IntGauge,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.active.dec();
        self.generation.connections.fetch_sub(1, Ordering::Relaxed);
    }
}
struct Binding {
    cancel: CancellationToken,
    masters: workers::Masters,
}
pub struct RunningProxy {
    quic_receive: Arc<Semaphore>,
    udp_buffers: Arc<Semaphore>,
    proxy_connections: Arc<Semaphore>,
    access_log: Option<Arc<access_log::Logger>>,
    current: ArcSwap<Snapshot>,
    path: PathBuf,
    _lock: storage::ConfigLock,
    artifacts: Arc<storage::ArtifactStore>,
    last_gc: Mutex<Option<serde_json::Value>>,
    registry: Arc<PluginRegistry>,
    coordinator: AsyncMutex<()>,
    bindings: Mutex<BTreeMap<String, Binding>>,
    connections: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
    health_capacity: Arc<Semaphore>,
    tasks: TaskTracker,
    quic_background: TaskTracker,
    shutdown: CancellationToken,
    force: CancellationToken,
    generations: Mutex<Vec<Weak<Snapshot>>>,
    probes: Mutex<BTreeMap<usize, Weak<PreparedUpstream>>>,
    pub metrics: Metrics,
    workers: Mutex<Arc<rolling::Generation>>,
    retiring: Mutex<Option<Arc<rolling::Generation>>>,
    worker_count: usize,
    observed: observability::Runtime,
}
pub struct ProxyBuilder {
    path: PathBuf,
    seed: Option<PathBuf>,
    registry: PluginRegistry,
}
impl ProxyBuilder {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            path: path.into(),
            seed: None,
            registry: PluginRegistry::new()?,
        })
    }
    pub fn plugins(mut self, registry: PluginRegistry) -> Self {
        self.registry = registry;
        self
    }
    /// Bootstrap a missing managed config from this source. An existing config
    /// always remains authoritative, including when it fails validation.
    pub fn seed(mut self, path: impl Into<PathBuf>) -> Self {
        self.seed = Some(path.into());
        self
    }
    pub async fn start(self) -> Result<Arc<RunningProxy>> {
        let path = match std::fs::canonicalize(&self.path) {
            Ok(path) => path,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.seed.is_some() => {
                // Do not replace a dangling symlink or create a partial config.
                ensure!(
                    std::fs::symlink_metadata(&self.path).is_err(),
                    "managed config path already exists"
                );
                let parent = self
                    .path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or_else(|| std::path::Path::new("."));
                std::fs::canonicalize(parent)
                    .context("managed config directory must exist")?
                    .join(
                        self.path
                            .file_name()
                            .context("managed config filename required")?,
                    )
            }
            Err(error) => return Err(error).context("config path must exist"),
        };
        let lock = storage::lock(&path)?;
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.seed.is_some() => {
                std::fs::read_to_string(self.seed.as_ref().unwrap()).context("read startup seed")?
            }
            Err(error) => return Err(error).context("read managed config"),
        };
        let raw = Config::parse(&text)?;
        let artifacts = Arc::new(storage::ArtifactStore::open(&raw.runtime.artifact_dir)?);
        let access_log = raw
            .runtime
            .access_log
            .as_ref()
            .map(|config| access_log::Logger::start(config, &path, &artifacts.path))
            .transpose()?;
        let metrics = Metrics::new()?;
        let worker_count = if raw.runtime.worker_local {
            if raw.runtime.io_workers == 0 {
                tokio::runtime::Handle::current()
                    .metrics()
                    .num_workers()
                    .clamp(1, 8)
            } else {
                raw.runtime.io_workers
            }
        } else {
            1
        };
        let workers = if raw.runtime.worker_local {
            Some(Arc::new(workers::Workers::new(
                worker_count,
                metrics.latency.clone(),
            )?))
        } else {
            None
        };
        let registry = Arc::new(self.registry);
        registry.observe_plugins(raw.runtime.observe_plugins);
        let r = registry.clone();
        let store = artifacts.clone();
        let snapshot = tokio::task::spawn_blocking(move || {
            prepare(storage::stage(raw, &store)?, &r, None, worker_count)
        })
        .await??;
        let mut sockets = Vec::new();
        let quic_background = TaskTracker::new();
        for l in &snapshot.config.listeners {
            sockets.push((
                l.id.clone(),
                workers::bind(l.address, l.protocol, workers.as_deref(), &quic_background).await?,
            ));
        }
        let metrics_socket = match snapshot.config.runtime.metrics_address {
            Some(address) => Some(TcpListener::bind(address).await?),
            None => None,
        };
        let encoded = snapshot.config.encode()?;
        let persist_path = path.clone();
        let store = artifacts.clone();
        ensure!(
            tokio::task::spawn_blocking(move || store.persist(&persist_path, encoded.as_bytes()))
                .await??,
            "initial directory fsync failed"
        );
        let runtime = snapshot.config.runtime.clone();
        let snapshot = Arc::new(snapshot);
        let generation = rolling::Generation::new(
            snapshot.config.generation,
            runtime.reload_mode == prost_config::ReloadMode::Rolling,
            workers,
            snapshot.clone(),
        );
        let p = Arc::new(RunningProxy {
            quic_receive: Arc::new(Semaphore::new(runtime.max_quic_receive_bytes)),
            udp_buffers: Arc::new(Semaphore::new(runtime.max_udp_buffer_bytes)),
            proxy_connections: Arc::new(Semaphore::new(runtime.max_proxy_connections)),
            access_log,
            current: ArcSwap::from(snapshot.clone()),
            path,
            _lock: lock,
            artifacts,
            last_gc: Mutex::new(None),
            registry,
            coordinator: AsyncMutex::new(()),
            bindings: Mutex::new(BTreeMap::new()),
            connections: Arc::new(Semaphore::new(runtime.max_connections)),
            requests: Arc::new(Semaphore::new(runtime.max_requests)),
            handshakes: Arc::new(Semaphore::new(256)),
            health_capacity: Arc::new(Semaphore::new(32)),
            tasks: TaskTracker::new(),
            quic_background,
            shutdown: CancellationToken::new(),
            force: CancellationToken::new(),
            generations: Mutex::new(vec![Arc::downgrade(&snapshot)]),
            probes: Mutex::new(BTreeMap::new()),
            metrics,
            workers: Mutex::new(generation.clone()),
            retiring: Mutex::new(None),
            worker_count,
            observed: observability::Runtime::default(),
        });
        p.metrics
            .registry
            .register(Box::new(observability::Exporter::new(Arc::downgrade(&p))?))?;
        for (id, socket) in sockets {
            p.start_listener(id, socket, generation.clone(), snapshot.clone());
        }
        p.start_health(&snapshot);
        if let Some(socket) = metrics_socket {
            p.start_metrics(socket);
        }
        Ok(p)
    }
}
fn prepare(
    config: Config,
    registry: &PluginRegistry,
    previous: Option<&Snapshot>,
    worker_count: usize,
) -> Result<Snapshot> {
    prepare_with_clients(config, registry, previous, worker_count, false)
}
fn prepare_with_clients(
    config: Config,
    registry: &PluginRegistry,
    previous: Option<&Snapshot>,
    worker_count: usize,
    reuse_clients: bool,
) -> Result<Snapshot> {
    config.validate()?;
    let static_ram = previous.map(|s| s.static_ram.clone()).unwrap_or_else(|| {
        Arc::new(prost_http::static_files::RamBudget::new(
            config.runtime.max_static_ram_bytes,
        ))
    });
    let fresh_clients = !reuse_clients
        && previous.is_some()
        && config.runtime.reload_mode == prost_config::ReloadMode::Rolling;
    let mut upstreams = BTreeMap::new();
    for (name, u) in &config.upstreams {
        if let Some(old) = previous.and_then(|s| s.upstreams.get(name)) {
            if !fresh_clients && serde_json::to_value(&old.config)? == serde_json::to_value(u)? {
                upstreams.insert(name.clone(), old.clone());
                continue;
            }
        }
        let old = previous
            .and_then(|s| s.upstreams.get(name))
            .map(Arc::as_ref);
        let tls = if let Some(old) = old.filter(|old| old.config.tls == u.tls) {
            old.tls.clone()
        } else {
            prost_transport::client_tls(u.tls.as_ref())?
        };
        let health_tls = if u.health_check.tls.is_none() {
            tls.clone()
        } else if let Some(old) =
            old.filter(|old| old.config.health_check.tls == u.health_check.tls)
        {
            old.health_tls.clone()
        } else {
            prost_transport::client_tls(u.health_check.tls.as_ref())?
        };
        let observed_group = old
            .map(|u| u.observed_group.clone())
            .unwrap_or_else(|| Arc::new(observability::Upstream::new(worker_count)));
        let dns = u
            .dns
            .as_ref()
            .map(|policy| {
                old.filter(|old| pool_compatible(&old.config, u))
                    .and_then(|old| old.dns.clone())
                    .map(Ok)
                    .unwrap_or_else(|| prost_transport::DnsResolver::new(policy.clone()))
            })
            .transpose()?;
        let client = if let Some(old) =
            old.filter(|old| !fresh_clients && pool_compatible(&old.config, u))
        {
            old.client.clone()
        } else {
            Arc::new(ClientPool::new_with_dns(
                u,
                tls.clone(),
                worker_count,
                observed_group.pool.clone(),
                dns.clone(),
            )?)
        };
        let balancer = balance::Balancer::new(u, old)?;
        let observed: Vec<_> = u
            .endpoints
            .iter()
            .map(|e| {
                old.and_then(|u| {
                    u.config
                        .endpoints
                        .iter()
                        .position(|p| p.id == e.id && p.address == e.address)
                        .map(|i| u.observed[i].clone())
                })
                .unwrap_or_else(|| Arc::new(observability::Endpoint::new(worker_count)))
            })
            .collect();
        upstreams.insert(
            name.clone(),
            Arc::new(PreparedUpstream {
                dns,
                http: u.http.as_ref().map(|policy| {
                    old.filter(|old| {
                        old.config.http.as_ref() == Some(policy) && pool_compatible(&old.config, u)
                    })
                    .and_then(|old| old.http.clone())
                    .unwrap_or_else(|| prost_http::HttpResources::new(policy.clone()))
                }),
                config: u.clone(),
                health_tls,
                tls,
                client,
                health: u
                    .endpoints
                    .iter()
                    .enumerate()
                    .map(|(index, endpoint)| {
                        let state = previous
                            .and_then(|s| s.upstreams.get(name))
                            .filter(|old| {
                                old.config.tls == u.tls
                                    && old.config.send_proxy_v2 == u.send_proxy_v2
                                    && old.config.health_check == u.health_check
                                    && old.config.http2 == u.http2
                            })
                            .and_then(|old| {
                                old.config
                                    .endpoints
                                    .iter()
                                    .position(|e| {
                                        e.id == endpoint.id && e.address == endpoint.address
                                    })
                                    .map(|i| {
                                        if old.config.passive_health == u.passive_health {
                                            old.health[i].snapshot()
                                        } else {
                                            old.health[i].reconfigure(u.passive_health.as_ref())
                                        }
                                    })
                            })
                            .unwrap_or_else(|| health::State::new(u.passive_health.as_ref()));
                        let mut state = state;
                        state.observed = observed[index].health.clone();
                        Arc::new(state)
                    })
                    .collect(),
                balancer,
                targets: u
                    .endpoints
                    .iter()
                    .map(|e| prost_http::Target::new(&e.address, u.tls.is_some()))
                    .collect::<Result<_>>()?,
                passive_statuses: u
                    .passive_health
                    .as_ref()
                    .filter(|p| p.max_fails > 0 && !p.failure_statuses.is_empty())
                    .map(|p| {
                        let mut mask = Box::new([0; 4]);
                        for code in &p.failure_statuses {
                            mask[(*code as usize - 400) / 64] |= 1 << ((*code - 400) % 64);
                        }
                        mask
                    }),
                observed,
                observed_group,
            }),
        );
    }
    let mut listeners = BTreeMap::new();
    for l in &config.listeners {
        let previous_l = previous.and_then(|s| s.listeners.get(&l.id));
        if let Some(old) = previous_l {
            let upstreams_unchanged = previous.is_some_and(|s| {
                s.upstreams
                    .iter()
                    .all(|(name, u)| upstreams.get(name).is_some_and(|new| Arc::ptr_eq(u, new)))
            });
            if (reuse_clients
                || !l
                    .routes
                    .iter()
                    .any(|r| r.static_files.as_ref().is_some_and(|f| f.load_to_ram)))
                && upstreams_unchanged
                && serde_json::to_value(&old.config)? == serde_json::to_value(l)?
            {
                listeners.insert(l.id.clone(), old.clone());
                continue;
            }
        }
        let capacity = previous_l
            .filter(|p| p.config.max_connections == l.max_connections)
            .map(|p| p.capacity.clone())
            .unwrap_or_else(|| Arc::new(Semaphore::new(l.max_connections)));
        let tls = if let Some(old) =
            previous_l.filter(|old| old.config.tls == l.tls && old.config.protocol == l.protocol)
        {
            old.tls.clone()
        } else {
            l.tls
                .as_ref()
                .map(|t| prost_transport::server_tls(t, l.protocol.is_http()))
                .transpose()?
        };
        let plugins = prepare_plugins(
            registry,
            &l.plugins,
            previous_l.map(|old| old.config.plugins.as_slice()),
            previous_l.map(|old| &old.plugins),
        )?;
        let certificate_expirations =
            if let Some(old) = previous_l.filter(|old| old.config.tls == l.tls) {
                old.certificate_expirations.clone()
            } else if let Some(tls) = &l.tls {
                let mut values = vec![(
                    "default".into(),
                    prost_transport::certificate_info(&std::fs::read(&tls.certificate)?)?.not_after,
                )];
                for cert in &tls.certificates {
                    values.push((
                        cert.server_names[0].clone(),
                        prost_transport::certificate_info(&std::fs::read(&cert.certificate)?)?
                            .not_after,
                    ));
                }
                values
            } else {
                Vec::new()
            };
        let has_hooks = plugins.iter().any(PreparedPlugin::has_event_hooks);
        let routes: Vec<Arc<Vec<PreparedPlugin>>> = l
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                let old = previous_l.filter(|old| {
                    old.config.routes.get(i).is_some_and(|route| {
                        route.host == r.host
                            && route.path_prefix == r.path_prefix
                            && route.priority == r.priority
                            && route.methods == r.methods
                            && route.headers == r.headers
                    })
                });
                prepare_plugins(
                    registry,
                    &r.plugins,
                    old.map(|old| old.config.routes[i].plugins.as_slice()),
                    old.map(|old| &old.routes[i]),
                )
            })
            .collect::<Result<_>>()?;
        let mut route_order: Vec<usize> = (0..l.routes.len()).collect();
        let route_matches = l
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                route_match::RouteMatch::compile(r)
                    .with_context(|| format!("listener {} route {i}", l.id))
            })
            .collect::<Result<Vec<_>>>()?;
        route_order.sort_by_key(|i| {
            (
                std::cmp::Reverse(l.routes[*i].priority),
                std::cmp::Reverse(l.routes[*i].path_prefix.len()),
                *i,
            )
        });
        let default_upstream = l.upstream.as_ref().map(|name| upstreams[name].clone());
        let uri_filter = plugins.iter().any(PreparedPlugin::uses_request_uri);
        let route_uri_filters = routes
            .iter()
            .map(|p| p.iter().any(PreparedPlugin::uses_request_uri))
            .collect();
        let route_upstreams = l
            .routes
            .iter()
            .map(|r| upstreams.get(&r.upstream).cloned())
            .collect();
        let static_files = l
            .routes
            .iter()
            .enumerate()
            .map(|(i, route)| {
                if reuse_clients {
                    if let Some(old) = previous_l.filter(|old| {
                        old.config
                            .routes
                            .get(i)
                            .is_some_and(|r| r.static_files == route.static_files)
                    }) {
                        return Ok(old.static_files[i].clone());
                    }
                }
                route
                    .static_files
                    .as_ref()
                    .map(|files| {
                        prost_http::static_files::StaticService::new(files, &static_ram)
                            .map(Arc::new)
                    })
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()?;
        listeners.insert(
            l.id.clone(),
            Arc::new(PreparedListener {
                http3: if l.protocol == Protocol::Http3 {
                    Some(http3::prepare(
                        l,
                        tls.as_ref().context("HTTP/3 requires TLS")?,
                        previous_l.map(Arc::as_ref),
                    )?)
                } else {
                    None
                },
                certificate_expirations,
                connection_admission: l
                    .plugins
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| {
                        matches!(p.name.as_str(), "connection_rate" | "connection_window")
                            .then_some(i)
                    })
                    .collect(),
                ip_capacity: previous_l
                    .map(|old| old.ip_capacity.clone())
                    .unwrap_or_default(),
                passthrough: if l.tls_passthrough {
                    Some(Arc::new(prost_transport::ClientHelloGuard::new(
                        l.tls_passthrough_settings.clone().unwrap_or_default(),
                    )?))
                } else {
                    None
                },
                config: l.clone(),
                tls,
                capacity,
                plugins,
                routes,
                route_index: route_index::RouteIndex::new(l, &route_order),
                route_matches,
                route_order,
                default_upstream,
                route_upstreams,
                static_files,
                has_hooks,
                uri_filter,
                route_uri_filters,
            }),
        );
    }
    let original_hash_key = upstreams.values().any(|u| u.balancer.needs_original());
    let encoded = config.encode()?;
    let hash = storage::hash(encoded.as_bytes());
    let artifact_hashes = storage::references(&config);
    Ok(Snapshot {
        static_ram,
        artifact_hashes,
        config,
        hash,
        upstreams,
        listeners,
        original_hash_key,
    })
}
fn pool_compatible(a: &Upstream, b: &Upstream) -> bool {
    if a.dns != b.dns {
        return false;
    }
    // Match every setting captured by prost_http::client. Selection, health and
    // per-request idle timeouts can change while the existing pool remains valid.
    a.tls == b.tls
        && a.send_proxy_v2 == b.send_proxy_v2
        && a.http2 == b.http2
        && a.connect_timeout_ms == b.connect_timeout_ms
        && a.write_timeout_ms == b.write_timeout_ms
        && a.observe_connection_reuse == b.observe_connection_reuse
        && a.observe_pool_state == b.observe_pool_state
        && a.pool_max_idle_per_host == b.pool_max_idle_per_host
        && a.pool_idle_timeout_ms == b.pool_idle_timeout_ms
        && a.http1_max_buf_size == b.http1_max_buf_size
        && a.http2_settings == b.http2_settings
        && a.endpoints.len() == b.endpoints.len()
        && a.endpoints.iter().all(|e| {
            b.endpoints
                .iter()
                .any(|n| n.id == e.id && n.address == e.address)
        })
}
fn prepare_plugins(
    registry: &PluginRegistry,
    config: &[prost_config::Plugin],
    old_config: Option<&[prost_config::Plugin]>,
    old_plugins: Option<&Arc<Vec<PreparedPlugin>>>,
) -> Result<Arc<Vec<PreparedPlugin>>> {
    if let (Some(old_config), Some(old_plugins)) = (old_config, old_plugins) {
        if old_config == config {
            return Ok(old_plugins.clone());
        }
    }
    Ok(Arc::new(
        config
            .iter()
            .enumerate()
            .map(|(i, p)| {
                if old_config.and_then(|old| old.get(i)) == Some(p) {
                    if let Some(prepared) = old_plugins.and_then(|old| old.get(i)) {
                        return Ok(prepared.clone());
                    }
                }
                registry.prepare(p)
            })
            .collect::<Result<_>>()?,
    ))
}
impl RunningProxy {
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.current.load_full()
    }
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
    pub fn force_token(&self) -> CancellationToken {
        self.force.clone()
    }
    pub fn status(&self) -> serde_json::Value {
        let snapshot = self.snapshot();
        let mut generations = self.generations.lock().unwrap();
        generations.retain(|g| g.strong_count() > 0);
        let upstreams: BTreeMap<_, _> = snapshot.upstreams.iter().map(|(name, u)| {
            let endpoints: Vec<_> = u.config.endpoints.iter().enumerate().map(|(i, e)| {
                serde_json::json!({"id":e.id,"enabled":e.enabled,"backup":e.backup,"max_active":e.max_active,"healthy":u.health[i].healthy.load(Ordering::Relaxed),"active":u.balancer.active(i),"passive_health":u.health[i].passive_status()})
            }).collect();
            (name, endpoints)
        }).collect();
        let balancers:BTreeMap<_,_>=snapshot.upstreams.iter().map(|(name,u)|(name,serde_json::json!({"algorithm":u.config.balance,"hash_key":if u.config.balance=="consistent_hash" {Some(u.config.hash_key.as_deref().unwrap_or("$remote_addr"))} else {None},"hash_points":u.balancer.points()}))).collect();
        let static_files:Vec<_>=snapshot.listeners.iter().flat_map(|(id,l)|l.static_files.iter().enumerate().filter_map(move |(i,files)|files.as_ref().map(|files|serde_json::json!({"listener":id,"route":i,"cache":files.stats()})))).collect();
        let worker = self.workers.lock().unwrap().clone();
        let retiring = self
            .retiring
            .lock()
            .unwrap()
            .as_ref()
            .filter(|g| !g.finished.is_cancelled())
            .map(|g| g.id);
        serde_json::json!({"generation": snapshot.config.generation, "hash": snapshot.hash, "live_generations": generations.len(), "connections": self.metrics.connections.get(), "shutting_down": self.shutdown.is_cancelled(), "worker_local": worker.workers.is_some(), "reload_mode":snapshot.config.runtime.reload_mode, "worker_generation":worker.id, "retiring_worker_generation":retiring, "worker_generations":self.worker_status(), "io_workers": self.worker_count, "upstreams": upstreams,"balancers":balancers, "artifact_gc":self.last_gc.lock().unwrap().clone(), "access_log": self.access_log.as_ref().map(|log|log.status()), "static_files":static_files,"static_files_ram":{"reserved_bytes":snapshot.static_ram.used(),"budget_bytes":snapshot.static_ram.limit()},"udp":self.udp_status()})
    }
    pub async fn validate(self: &Arc<Self>, text: String) -> Result<()> {
        let p = self.clone();
        // A cancelled caller cannot release the coordinator while blocking
        // artifact staging is still running and a GC could otherwise start.
        tokio::spawn(async move { p.validate_inner(text).await }).await?
    }
    pub async fn access_log_control(
        self: &Arc<Self>,
        reopen: bool,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let p = self.clone();
        tokio::spawn(async move {
            let _guard = p.coordinator.lock().await;
            ensure!(!p.shutdown.is_cancelled(), "proxy shutting down");
            ensure!(
                p.snapshot().config.generation == expected,
                "generation_conflict"
            );
            let log = p.access_log.as_ref().context("access log is disabled")?;
            let epoch = log.control(reopen).await?;
            Ok(serde_json::json!({"reopened":reopen,"epoch":epoch,"generation":expected}))
        })
        .await?
    }
    async fn validate_inner(self: &Arc<Self>, text: String) -> Result<()> {
        let mut observation = observability::Transaction::new(
            &self.observed,
            &self.metrics.transaction_latency[2],
            2,
        );
        let result = async {
            let _guard = self.coordinator.lock().await;
            let old = self.snapshot();
            let config = Config::parse(&text)?;
            self.compatible(&config, &old)?;
            let registry = self.registry.clone();
            let worker_count = self.worker_count;
            let store = self.artifacts.clone();
            tokio::task::spawn_blocking(move || {
                prepare(
                    storage::stage(config, &store)?,
                    &registry,
                    Some(&old),
                    worker_count,
                )
            })
            .await??;
            Ok(())
        }
        .await;
        observation.finish(&result, true);
        result
    }
    /// Explicit, bounded collection; no file watcher or periodic config scan.
    pub async fn collect_artifacts(
        self: &Arc<Self>,
        options: GcOptions,
        expected: u64,
    ) -> Result<GcReport> {
        let p = self.clone();
        tokio::spawn(async move {
            let _timer = p.metrics.gc_seconds.start_timer();
            let result = p.collect_artifacts_inner(options, expected).await;
            let outcome = match &result {
                Ok(report) if report.errors == 0 => "success",
                Ok(_) => "partial",
                Err(_) => "error",
            };
            p.metrics
                .gc_runs
                .with_label_values(&[
                    if options.dry_run {
                        "dry_run"
                    } else {
                        "collect"
                    },
                    outcome,
                ])
                .inc();
            if let Ok(report) = &result {
                p.metrics.gc_files.inc_by(report.deleted_files);
                p.metrics.gc_bytes.inc_by(report.deleted_bytes);
            }
            result
        })
        .await?
    }
    async fn collect_artifacts_inner(
        self: &Arc<Self>,
        options: GcOptions,
        expected: u64,
    ) -> Result<GcReport> {
        let _guard = self.coordinator.lock().await;
        ensure!(!self.shutdown.is_cancelled(), "proxy shutting down");
        let current = self.snapshot();
        ensure!(current.config.generation == expected, "generation_conflict");
        let live: Vec<_> = {
            let mut generations = self.generations.lock().unwrap();
            generations.retain(|generation| generation.strong_count() > 0);
            generations.iter().filter_map(Weak::upgrade).collect()
        };
        let store = self.artifacts.clone();
        let path = self.path.clone();
        let report = tokio::task::spawn_blocking(move || {
            let mut file = File::open(&path)?;
            ensure!(
                file.metadata()?.len() <= 4 * 1024 * 1024,
                "managed config exceeds GC read limit"
            );
            let mut bytes = Vec::new();
            (&mut file)
                .take(4 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                bytes.len() <= 4 * 1024 * 1024,
                "managed config exceeds GC read limit"
            );
            ensure!(
                storage::hash(&bytes) == current.hash,
                "managed_config_changed: use Runtime API before collecting artifacts"
            );
            // Repair/confirm durability before removing files required by an
            // older on-disk config after a previously failed directory fsync.
            if !options.dry_run {
                file.sync_all()?;
                File::open(path.parent().context("config parent")?)?.sync_all()?;
            }
            let mut protected = std::collections::HashSet::new();
            for snapshot in live.iter().chain(std::iter::once(&current)) {
                protected.extend(snapshot.artifact_hashes.iter().cloned());
            }
            if path.parent() == Some(store.path.as_path()) {
                if let Some(name) = path.file_name() {
                    protected.insert(name.to_os_string());
                }
            }
            store.collect(&protected, options, current.config.generation, live.len())
        })
        .await??;
        *self.last_gc.lock().unwrap() = Some(report.json());
        Ok(report)
    }
    fn compatible(&self, config: &Config, old: &Snapshot) -> Result<()> {
        ensure!(!self.shutdown.is_cancelled(), "proxy shutting down");
        let mut runtime = config.runtime.clone();
        runtime.s3 = old.config.runtime.s3.clone();
        ensure!(
            runtime == old.config.runtime,
            "runtime settings require restart"
        );
        for l in &config.listeners {
            for previous in &old.config.listeners {
                if previous.address == l.address
                    && previous.protocol.is_udp() == l.protocol.is_udp()
                {
                    ensure!(
                        previous.id == l.id && previous.protocol == l.protocol,
                        "listener identity/protocol change on same address requires restart"
                    );
                }
            }
            if let Some(previous) = old.listeners.get(&l.id) {
                ensure!(
                    previous.config.protocol == l.protocol,
                    "listener protocol change requires restart"
                );
                ensure!(
                    previous.config.address == l.address,
                    "listener address change requires new id"
                );
            }
        }
        Ok(())
    }
    /// Detached coordinator task completes commits even if the requesting client disconnects.
    pub async fn apply(self: &Arc<Self>, text: String, expected: u64) -> Result<serde_json::Value> {
        let p = self.clone();
        tokio::spawn(async move {
            let mut observation =
                observability::Transaction::new(&p.observed, &p.metrics.transaction_latency[0], 0);
            let result = p.commit(text, expected).await;
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
    pub async fn set_enabled(
        self: &Arc<Self>,
        endpoint: String,
        enabled: bool,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let p = self.clone();
        tokio::spawn(async move {
            let mut observation =
                observability::Transaction::new(&p.observed, &p.metrics.transaction_latency[1], 1);
            let result = async {
                let _guard = p.coordinator.lock().await;
                let snapshot = p.snapshot();
                ensure!(
                    snapshot.config.generation == expected,
                    "generation_conflict"
                );
                let (upstream, id) = endpoint
                    .split_once('/')
                    .context("endpoint must be upstream/id")?;
                let mut config = snapshot.config.clone();
                let e = config
                    .upstreams
                    .get_mut(upstream)
                    .context("unknown upstream")?
                    .endpoints
                    .iter_mut()
                    .find(|e| e.id == id)
                    .context("unknown endpoint")?;
                e.enabled = enabled;
                p.commit_locked(config, expected).await
            }
            .await;
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
    async fn commit(self: &Arc<Self>, text: String, expected: u64) -> Result<serde_json::Value> {
        let _guard = self.coordinator.lock().await;
        self.commit_locked(Config::parse(&text)?, expected).await
    }
    async fn commit_locked(
        self: &Arc<Self>,
        mut config: Config,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let old = self.snapshot();
        ensure!(old.config.generation == expected, "generation_conflict");
        self.compatible(&config, &old)?;
        config.generation = expected.checked_add(1).context("generation overflow")?;
        let registry = self.registry.clone();
        let previous = old.clone();
        let worker_count = self.worker_count;
        let store = self.artifacts.clone();
        let new = Arc::new(
            tokio::task::spawn_blocking(move || {
                prepare(
                    storage::stage(config, &store)?,
                    &registry,
                    Some(&previous),
                    worker_count,
                )
            })
            .await??,
        );
        let previous_generation = self.workers.lock().unwrap().clone();
        let rolling = new.config.runtime.reload_mode == prost_config::ReloadMode::Rolling;
        let next_generation = if rolling {
            let retiring = self.retiring.lock().unwrap().clone();
            if let Some(retiring) = retiring {
                ensure!(retiring.finished.is_cancelled() || !retiring.unlimited(),
                    "rolling_generation_busy: an unlimited-drain generation is still active; TLS-only updates remain available");
                retiring.finished.cancelled().await;
                self.retiring.lock().unwrap().take();
            }
            let workers = if previous_generation.workers.is_some() {
                let count = self.worker_count;
                let latency = self.metrics.latency.clone();
                Some(Arc::new(
                    tokio::task::spawn_blocking(move || workers::Workers::new(count, latency))
                        .await??,
                ))
            } else {
                None
            };
            rolling::Generation::new(new.config.generation, true, workers, new.clone())
        } else {
            previous_generation.clone()
        };
        let mut sockets = Vec::new();
        for listener in &new.config.listeners {
            if rolling || !old.listeners.contains_key(&listener.id) {
                let attached = if rolling {
                    let bindings = self.bindings.lock().unwrap();
                    bindings
                        .get(&listener.id)
                        .map(|binding| {
                            workers::attach(&binding.masters, next_generation.workers.as_deref())
                        })
                        .transpose()?
                } else {
                    None
                };
                let socket = match attached {
                    Some(socket) => socket,
                    None => workers::bind(
                        listener.address,
                        listener.protocol,
                        next_generation.workers.as_deref(),
                        &self.quic_background,
                    )
                    .await
                    .with_context(|| format!("bind {}", listener.address))?,
                };
                sockets.push((listener.id.clone(), socket));
            }
        }
        if let Some(workers) = &next_generation.workers {
            workers.ready().await?;
        }
        let path = self.path.clone();
        let encoded = new.config.encode()?;
        let store = self.artifacts.clone();
        let durable =
            tokio::task::spawn_blocking(move || store.persist(&path, encoded.as_bytes())).await??;
        // No fallible preparation past this point.
        self.current.store(new.clone());
        self.refresh_quic_tls(&new);
        {
            let mut generations = self.generations.lock().unwrap();
            generations.retain(|generation| generation.strong_count() > 0);
            generations.push(Arc::downgrade(&new));
        }
        if rolling {
            *self.workers.lock().unwrap() = next_generation.clone();
            let old_bindings = std::mem::take(&mut *self.bindings.lock().unwrap());
            for (id, socket) in sockets {
                self.start_listener(id, socket, next_generation.clone(), new.clone());
            }
            for binding in old_bindings.values() {
                binding.cancel.cancel();
            }
            self.retire_generation(previous_generation, new.config.runtime.drain_seconds)
                .await;
        } else {
            self.bindings.lock().unwrap().retain(|id, binding| {
                if !new.listeners.contains_key(id) {
                    binding.cancel.cancel();
                    false
                } else {
                    true
                }
            });
            for (id, socket) in sockets {
                self.start_listener(id, socket, next_generation.clone(), new.clone());
            }
        }
        self.start_health(&new);
        self.metrics.commits.inc();
        tracing::info!(
            generation = new.config.generation,
            durable,
            "configuration committed"
        );
        Ok(
            serde_json::json!({"generation":new.config.generation,"hash":new.hash,"durable":durable}),
        )
    }
    async fn retire_generation(
        self: &Arc<Self>,
        generation: Arc<rolling::Generation>,
        seconds: u64,
    ) {
        generation.accepting.cancel();
        generation.begin_drain(seconds);
        generation.accepts.close();
        generation.accepts.wait().await;
        generation.snapshot.store(None);
        generation.tasks.close();
        *self.retiring.lock().unwrap() = Some(generation.clone());
        let weak = Arc::downgrade(self);
        self.tasks.spawn(async move {
            let expired = if generation.unlimited() {
                generation.tasks.wait().await;
                false
            } else {
                tokio::time::timeout(generation.remaining(), generation.tasks.wait())
                    .await
                    .is_err()
            };
            if expired {
                if let Some(p) = weak.upgrade() {
                    p.observed.worker_retirements[1].fetch_add(1, Ordering::Relaxed);
                    p.observed.worker_retirements[2].fetch_add(
                        generation.connections.load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                }
                generation.force.cancel();
                generation.tasks.wait().await;
            }
            if let Some(workers) = generation.workers.clone() {
                let _ = tokio::task::spawn_blocking(move || workers.join()).await;
            }
            if let Some(p) = weak.upgrade() {
                p.observed.worker_retirements[0].fetch_add(1, Ordering::Relaxed);
            }
            generation.finished.cancel();
        });
    }
    fn start_listener(
        self: &Arc<Self>,
        id: String,
        sockets: workers::BoundListener,
        generation: Arc<rolling::Generation>,
        _pinned: Arc<Snapshot>,
    ) {
        let cancel = self.shutdown.child_token();
        self.bindings.lock().unwrap().insert(
            id.clone(),
            Binding {
                cancel: cancel.clone(),
                masters: sockets.masters,
            },
        );
        if let Some((socket, sessions)) = sockets.udp {
            self.start_udp(id, socket, sessions, generation, cancel);
            return;
        }
        if let Some(endpoint) = sockets.quic {
            self.start_http3(id, endpoint, generation, cancel, &_pinned);
            return;
        }
        for (handle, socket) in sockets.sockets {
            let weak = Arc::downgrade(self);
            let id = id.clone();
            let cancel = cancel.clone();
            let generation = generation.clone();
            let accepts = generation.accepts.clone();
            self.tasks.spawn_on(accepts.track_future(async move {
                loop {
                    let accepted=tokio::select!{biased;_=cancel.cancelled()=>break,_=generation.accepting.cancelled()=>break,a=socket.accept()=>a};
                    let (stream,peer)=match accepted {Ok(value)=>value,Err(error)=>{tracing::warn!(%error,"accept failed");tokio::time::sleep(Duration::from_millis(100)).await;continue;}};
                    let Some(p)=weak.upgrade() else{break;};
                    let snapshot=if generation.rolling { generation.snapshot.load_full().expect("accept snapshot retained until accepts stop") } else { p.snapshot() };
                    let Some(listener)=snapshot.listeners.get(&id).cloned() else{continue;};
                    let global=match p.connections.clone().try_acquire_owned(){Ok(permit)=>permit,Err(_)=>continue};
                    let local=match listener.capacity.clone().try_acquire_owned(){Ok(permit)=>permit,Err(_)=>continue};
                    let plugin_permits=match connection_permits(&listener.plugins){Ok(permits)=>permits,Err(_)=>continue};
                    p.metrics.connections.inc();
                    generation.connections.fetch_add(1, Ordering::Relaxed);
                    let audit=if listener.config.protocol==Protocol::Tcp && listener.config.access_log {p.access_log.as_ref().filter(|log|log.sample()).map(|log|access_log::Ticket::tcp(log.clone(),&peer.to_string(),&id,snapshot.config.generation))}else{None};
                    let lease=ConnectionLease{generation:generation.clone(),_ip:None,audit,_global:global,_local:local,_plugins:plugin_permits,active:p.metrics.connections.clone()};
                    let p2=p.clone();let id=id.clone();let audit=lease.audit.clone();let force=generation.force.clone();
                    p.tasks.spawn(generation.tasks.track_future(async move {
                        let result=tokio::select!{_=p2.force.cancelled()=>Ok(()),_=force.cancelled()=>Ok(()),r=p2.connection(id,stream,peer,snapshot,listener,lease)=>r};
                        if let Some(audit)=&audit {audit.finish(None,if result.is_err(){"error"}else{"aborted"});}
                        if let Err(error)=result {p2.metrics.errors.inc();tracing::debug!(%error,%peer,"connection ended with error");}
                    }));
                }
            }),&handle);
        }
    }
    async fn connection(
        self: &Arc<Self>,
        id: String,
        stream: TcpStream,
        mut peer: SocketAddr,
        snapshot: Arc<Snapshot>,
        listener: Arc<PreparedListener>,
        mut lease: ConnectionLease,
    ) -> Result<()> {
        let mut addresses = prost_transport::ProxyAddresses {
            source: peer,
            destination: stream.local_addr()?,
        };
        prost_transport::configure_socket(&stream)?;
        let mut io: prost_transport::BoxStream = Box::new(stream);
        let transport_peer = peer;
        if let Some(config) = &listener.config.proxy_protocol {
            addresses =
                prost_transport::read_proxy_addresses(&mut io, peer, addresses.destination, config)
                    .await?;
            peer = addresses.source;
            if let Some(audit) = &lease.audit {
                audit.peer(&peer.to_string());
            }
        }
        lease._ip = Some(
            listener
                .ip_capacity
                .acquire(peer.ip(), listener.config.max_connections_per_ip)?,
        );
        if listener.config.protocol == Protocol::Tcp {
            invoke_http_plugins(
                &listener.plugins,
                Phase::new("connection", peer.ip()),
                None,
                None,
            )
            .await?;
        } else {
            for index in &listener.connection_admission {
                invoke_http_plugins(
                    std::slice::from_ref(&listener.plugins[*index]),
                    Phase::new("connection", peer.ip()),
                    None,
                    None,
                )
                .await?;
            }
        }
        let mut selected_upstream = listener.config.upstream.clone();
        if let Some(guard) = &listener.passthrough {
            let inspected = guard.inspect_routed(io).await?;
            io = inspected.stream;
            if let Some(upstream) = inspected.upstream {
                selected_upstream = Some(upstream);
            }
        }
        if let Some(tls) = &listener.tls {
            let _permit = self
                .handshakes
                .clone()
                .try_acquire_owned()
                .inspect_err(|_| {
                    self.observed.tls[3].fetch_add(1, Ordering::Relaxed);
                })
                .context("handshake capacity")?;
            let mut timer =
                observability::Handshake::new(&self.observed, &self.metrics.tls_latency);
            let result = tokio::time::timeout(
                Duration::from_millis(listener.config.tls_handshake_timeout_ms),
                tokio_rustls::TlsAcceptor::from(tls.clone()).accept(io),
            )
            .await;
            io = Box::new(match result {
                Ok(Ok(stream)) => {
                    timer.finish(0);
                    let mut stream = stream;
                    stream.get_mut().1.set_buffer_limit(Some(
                        listener.config.tls.as_ref().unwrap().buffer_limit_bytes,
                    ));
                    stream
                }
                Ok(Err(error)) => {
                    timer.finish(1);
                    return Err(error.into());
                }
                Err(error) => {
                    timer.finish(2);
                    return Err(error.into());
                }
            });
        }
        let mut event = Event {
            abi_version: 1,
            hook: "connection".into(),
            peer: peer.to_string(),
            upstream: selected_upstream,
            headers: BTreeMap::new(),
            config: serde_json::Value::Null,
        };
        if let Some(ms) = listener.config.client_write_timeout_ms.filter(|ms| *ms > 0) {
            io = Box::new(prost_transport::TimedWrite::new(
                io,
                Duration::from_millis(ms),
            ));
        }
        if listener.config.protocol == Protocol::Tcp || listener.has_hooks {
            invoke(&listener.plugins, &mut event, None).await?;
        }
        if listener.config.protocol == Protocol::Tcp {
            event.hook = "upstream".into();
            invoke(&listener.plugins, &mut event, None).await?;
            let u = snapshot
                .upstreams
                .get(event.upstream.as_ref().context("no upstream selected")?)
                .context("plugin selected unknown upstream")?;
            let selection = u
                .balancer
                .select_tcp(u, peer.ip())
                .map_err(|e| u.selection_error(e))?;
            let index = selection.index;
            ensure!(
                !listener.config.tls_passthrough || u.config.tls.is_none(),
                "passthrough plugin selected a TLS-wrapped upstream"
            );
            let _proxy_permit = if u.config.send_proxy_v2 {
                Some(
                    self.proxy_connections
                        .clone()
                        .try_acquire_owned()
                        .context("PROXY transport capacity")?,
                )
            } else {
                None
            };
            if let Some(audit) = &lease.audit {
                audit.upstream(
                    event.upstream.as_deref(),
                    Some(&u.config.endpoints[index].address),
                );
            }
            u.observed[index].selected();
            let backend = match prost_transport::connect_tcp_resolved(
                &u.config.endpoints[index].address,
                &u.config,
                u.tls.clone(),
                Some(addresses),
                &u.observed[index].tcp_retries,
                u.dns.as_deref(),
            )
            .await
            {
                Ok(v) => v,
                Err(e) => {
                    if let Some(audit) = &lease.audit {
                        audit.finish(None, "error");
                    }
                    u.observed[index].error();
                    u.health[index].passive_failure();
                    return Err(e);
                }
            };
            let result = prost_transport::relay_with_timeouts(
                io,
                backend,
                Duration::from_millis(u.config.idle_timeout_ms),
                u.config
                    .half_close_timeout_ms
                    .filter(|ms| *ms > 0)
                    .map(Duration::from_millis),
            )
            .await;
            if let Ok(bytes) = &result {
                self.metrics.bytes.inc_by(*bytes);
            }
            if let Some(audit) = &lease.audit {
                audit.finish(
                    result.as_ref().ok().copied(),
                    if result.is_ok() { "complete" } else { "error" },
                );
            }
            drop(selection);
            event.hook = "end".into();
            invoke(&listener.plugins, &mut event, None).await?;
            return result.map(|_| ());
        }
        let info = Arc::new(ConnectionInfo {
            snapshot: lease.generation.rolling.then(|| snapshot.clone()),
            addresses,
            proxy_clients: proxy_clients::Clients::new(self.proxy_connections.clone()),
            transport_peer,
            headers_seen: std::sync::atomic::AtomicBool::new(false),
            first_headers: tokio::sync::Notify::new(),
            peer,
            peer_text: event.peer,
            forwarded_for: HeaderValue::from_str(&peer.ip().to_string())?,
            forwarded_proto: HeaderValue::from_static(if listener.tls.is_some() {
                "https"
            } else {
                "http"
            }),
            _lease: lease,
        });
        let id: Arc<str> = id.into();
        let h2 = listener.config.http2_settings.clone();
        let header_timeout = listener.config.header_read_timeout_ms;
        let keepalive_timeout = listener.config.keepalive_idle_timeout_ms.unwrap_or(0);
        drop(snapshot);
        drop(listener);
        if self.access_log.is_some() {
            Box::pin(self.serve_http::<LoggedHttp>(
                io,
                info,
                id,
                h2,
                header_timeout,
                keepalive_timeout,
            ))
            .await
        } else {
            self.serve_http::<PlainHttp>(io, info, id, h2, header_timeout, keepalive_timeout)
                .await
        }
    }
    async fn serve_http<M: HttpMode>(
        self: &Arc<Self>,
        io: prost_transport::BoxStream,
        info: Arc<ConnectionInfo>,
        id: Arc<str>,
        h2: prost_config::Http2Settings,
        header_timeout: u64,
        keepalive_timeout: u64,
    ) -> Result<()> {
        let io: prost_transport::BoxStream = if h2.header_read_timeout_ms > 0 {
            Box::new(prost_transport::H2HeaderDeadline::new(
                io,
                Duration::from_millis(h2.header_read_timeout_ms),
            ))
        } else {
            io
        };
        let stream_write_timeout = h2.stream_write_timeout_ms;
        let (io, clock) = if keepalive_timeout > 0 {
            let (io, clock) =
                prost_http::keepalive::HeaderClock::wrap(io, header_timeout, keepalive_timeout);
            (io, Some(clock))
        } else {
            (io, None)
        };
        let p = self.clone();
        let drain = info._lease.generation.drain.clone();
        let first_info = info.clone();
        let service = service_fn(move |request| {
            if !info.headers_seen.load(Ordering::Relaxed) {
                info.headers_seen.store(true, Ordering::Relaxed);
                info.first_headers.notify_one();
            }
            let p = p.clone();
            let id = id.clone();
            let info = info.clone();
            M::response(p, id, info, request, stream_write_timeout > 0)
        });
        let mut builder = auto::Builder::new(prost_http::h2_deadline::H2Executor::new(
            stream_write_timeout,
            self.observed.h2_write_timeouts.clone(),
        ));
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout((header_timeout > 0 || keepalive_timeout > 0).then(|| {
                Duration::from_millis(if header_timeout > 0 {
                    header_timeout
                } else {
                    keepalive_timeout
                })
            }))
            .max_buf_size(32768);
        if let Some(clock) = &clock {
            builder.http1().timer(clock.clone());
        }
        builder
            .http2()
            .timer(TokioTimer::new())
            .initial_stream_window_size(h2.initial_stream_window_size)
            .initial_connection_window_size(h2.initial_connection_window_size)
            .adaptive_window(h2.adaptive_window)
            .max_concurrent_streams(h2.max_concurrent_streams)
            .max_header_list_size(32768)
            .max_send_buf_size(32768);
        let connection = builder.serve_connection_with_upgrades(TokioIo::new(io), service);
        tokio::pin!(connection);
        if header_timeout > 0 || first_info._lease.generation.rolling {
            // A socket accepted just before a rolling switch may not have its
            // first headers yet. Calling graceful_shutdown during auto protocol
            // detection drops that legitimate request without a response. Let
            // the first request start, bounded by header/drain deadlines, then
            // drain below once Hyper knows which request/stream to finish.
            tokio::select! {
                result = &mut connection => { result.map_err(|e| anyhow::anyhow!(e))?; return Ok(()); },
                _ = first_info.first_headers.notified() => {},
                _ = async {if header_timeout > 0 {tokio::time::sleep(Duration::from_millis(header_timeout)).await;} else {std::future::pending::<()>().await;}} => anyhow::bail!("initial request headers timeout"),
                _ = self.shutdown.cancelled() => {
                    connection.as_mut().graceful_shutdown();
                    connection.await.map_err(|e| anyhow::anyhow!(e))?;
                    return Ok(());
                },
            }
        }
        drop(first_info);
        tokio::select! {
            result = &mut connection => { result.map_err(|e| anyhow::anyhow!(e))?; },
            _ = async {if let Some(clock)=&clock {clock.expired().await;}else{std::future::pending::<()>().await;}} => {connection.as_mut().graceful_shutdown();connection.await.map_err(|e|anyhow::anyhow!(e))?;},
            _ = async {tokio::select!{_=self.shutdown.cancelled()=>{},_=drain.cancelled()=>{}}} => { connection.as_mut().graceful_shutdown(); connection.await.map_err(|e| anyhow::anyhow!(e))?; },
        }
        Ok(())
    }
    async fn handle_http_logged(
        self: Arc<Self>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        request: Request<IncomingBody>,
        h2_write_guard: bool,
    ) -> Response<Body> {
        use hyper::body::Body as _;
        let snapshot = info.snapshot.clone().unwrap_or_else(|| self.snapshot());
        let route = snapshot
            .listeners
            .get(id.as_ref())
            .and_then(|l| select_route_index(l, &request));
        let enabled = snapshot.listeners.get(id.as_ref()).is_some_and(|l| {
            route
                .and_then(|i| l.config.routes[i].access_log)
                .unwrap_or(l.config.access_log)
        });
        let log = self.access_log.as_ref().unwrap();
        let audit = if enabled && log.path_enabled(request.uri().path()) && log.sample() {
            let path = if log.config.include_query {
                request.uri().path_and_query().map_or("/", |p| p.as_str())
            } else {
                request.uri().path()
            };
            Some(access_log::Ticket::http(
                log.clone(),
                &info.peer_text,
                &id,
                snapshot.config.generation,
                request.method().as_str(),
                format!("{:?}", request.version()),
                path,
            ))
        } else {
            None
        };
        let head = request.method() == http::Method::HEAD;
        // Pass the already selected snapshot directly. Extensions would allocate
        // a map and a type-erased context box even for sampled-out requests.
        let context = access_log::RequestContext {
            snapshot,
            route,
            audit: audit.clone(),
        };
        let mut response = self.handle_http(id, info, request, Some(context)).await;
        if h2_write_guard {
            prost_http::h2_deadline::observe_response(&mut response);
        }
        let Some(audit) = audit else {
            return response;
        };
        audit.headers(response.status().as_u16());
        if audit.is_tunnel() {
            return response;
        }
        if head
            || response.status().is_informational()
            || matches!(response.status().as_u16(), 204 | 304)
            || response.body().is_end_stream()
        {
            audit.finish(Some(0), "complete");
            return response;
        }
        response.map(|body| body.observe(audit))
    }
    async fn handle_http(
        self: Arc<Self>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        mut request: Request<IncomingBody>,
        context: Option<access_log::RequestContext>,
    ) -> Response<Body> {
        self.metrics.requests.inc();
        let worker_generation = info._lease.generation.clone();
        let _timer = local_metrics::Timer::new(&self.metrics.latency);
        let (snapshot, route_override, audit) = match context {
            Some(context) => (context.snapshot, Some(context.route), context.audit),
            None => (
                info.snapshot.clone().unwrap_or_else(|| self.snapshot()),
                None,
                None,
            ),
        };
        let peer = info.peer.ip();
        let downstream_version = request.version();
        let head_request = request.method() == http::Method::HEAD;
        let mut header_context = None;
        let mut selected_route = None;
        let result = async {
            let permit = self.requests.clone().try_acquire_owned().context("request capacity")?;
            let l = snapshot.listeners.get(id.as_ref()).context("listener removed")?;
            let route_index = route_override.unwrap_or_else(|| select_route_index(l,&request));
            let selected = route_index.map(|i| l.route_upstreams[i].as_ref()).unwrap_or(l.default_upstream.as_ref());
            let static_files = route_index.and_then(|i| l.static_files[i].as_ref());
            selected_route = route_index;
            let route_plugins = route_index.map(|i|l.routes[i].as_slice());
            let overrides = route_plugins.and_then(|ps|ps.iter().find_map(PreparedPlugin::parent_override)).unwrap_or_default();
            let needs = l.plugins.iter().chain(route_plugins.unwrap_or_default()).fold(0,|bits,p|bits|p.context_needs());
            if needs != 0 {
                let request_id = if needs & prost_plugin::NEEDS_REQUEST_ID != 0 {
                    let mut bytes=[0u8;16];getrandom::fill(&mut bytes).map_err(|e|anyhow::anyhow!("request id random source: {e}"))?;
                    let mut text=[0u8;32];let hex=b"0123456789abcdef";for(i,b)in bytes.iter().enumerate(){text[2*i]=hex[(b>>4)as usize];text[2*i+1]=hex[(b&15)as usize];}Some(HeaderValue::from_bytes(&text)?)
                } else {None};
                let mut request_headers=http::HeaderMap::new();
                if needs & prost_plugin::NEEDS_REQUEST_HEADERS != 0 {for p in l.plugins.iter().chain(route_plugins.unwrap_or_default()){p.copy_header_inputs(false,request.headers(),&mut request_headers);}}
                header_context=Some(HeaderContext{remote_addr:info.forwarded_for.clone(),scheme:if l.tls.is_some(){"https"}else{"http"},method:request.method().clone(),uri:request.uri().clone(),current_uri:None,host:HeaderValue::from_str(&request_host(&request).to_ascii_lowercase())?,request_id,request_headers,upstream_addr:None,upstream_headers:http::HeaderMap::new()});
            }
            if !l.config.trusted_proxies.iter().any(|cidr| cidr.contains(&info.transport_peer.ip())) {
                let names: Vec<_> = request.headers().keys().filter(|n| n.as_str() == "forwarded" || n.as_str().starts_with("x-forwarded-")).cloned().collect();
                for name in names { request.headers_mut().remove(name); }
            }
            request.headers_mut().append(FORWARDED_FOR.clone(),info.forwarded_for.clone());
            request.headers_mut().insert(FORWARDED_PROTO.clone(),info.forwarded_proto.clone());
            let has_hooks = l.has_hooks || route_plugins.is_some_and(|plugins| plugins.iter().any(PreparedPlugin::has_event_hooks));
            let end_hook = l.plugins.iter().any(|p| p.has_hook("end")) || route_plugins.is_some_and(|plugins| plugins.iter().any(|p| p.has_hook("end")));
            let mut event = if has_hooks {
                let upstream = route_index.map(|i|l.config.routes[i].upstream.clone()).or_else(||l.config.upstream.clone()).filter(|name| !name.is_empty());
                let event = Event { abi_version:1, hook:"request_headers".into(), peer:info.peer_text.clone(), upstream, headers:BTreeMap::new(),config:serde_json::Value::Null };
                Some(event)
            } else { None };
            let phase = Phase{hook:"request_headers",peer,status:200,context:header_context.as_ref(),skip_headers:overrides.0};
            if l.uri_filter || route_index.is_some_and(|i|l.route_uri_filters[i]) {
                if header_context.is_none() && (selected.is_some_and(|u|u.balancer.needs_original()) || (has_hooks && snapshot.original_hash_key)) {
                    if let Some(original)=request.uri().path_and_query().cloned() {request.extensions_mut().insert(balance::OriginalPath(original));}
                }
                let (uri, result) = Box::pin(invoke_uri_plugins(&l.plugins, route_plugins, phase, event.as_mut(), &mut request)).await;
                if let Some(ctx)=&mut header_context {ctx.current_uri=Some(Box::new(uri.clone()));}
                *request.uri_mut()=uri;
                result?;
            } else {
                invoke_http_plugins(&l.plugins,phase,event.as_mut(),Some(request.headers_mut())).await?;
                if let Some(plugins)=route_plugins {invoke_http_plugins(plugins,phase.child(),event.as_mut(),Some(request.headers_mut())).await?;}
            }
            if selected.is_none() && static_files.is_none() {
                let mut response=prost_http::response(404,"no matching route\n");
                apply_local_headers(&mut response,&snapshot,&id,selected_route,peer,header_context.as_ref(),(head_request,downstream_version)).await;
                return Ok(response);
            }
            if let Some(event) = event.as_mut().filter(|_| static_files.is_none()) {
                event.hook = "upstream".into();
                invoke(&l.plugins,event,None).await?;
                if let Some(plugins) = route_plugins { invoke(plugins,event,None).await?; }
            }
            let u = if static_files.is_some() { None } else { Some(if let Some(event) = &event {
                snapshot.upstreams.get(event.upstream.as_ref().context("no upstream")?).context("plugin selected unknown upstream")?
            } else { selected.context("no matching upstream")? }) };
            // Local protocol rejections must not be counted as backend failures.
            anyhow::ensure!(request.method() != http::Method::CONNECT, "CONNECT unsupported");
            let route = route_index.map(|i| &l.config.routes[i]);
            let request_policy = prost_http::RequestPolicy {
                max_bytes: route.and_then(|r| r.max_request_body_bytes).or(l.config.max_request_body_bytes),
                idle_timeout_ms: route.and_then(|r| r.request_body_timeout_ms).or(l.config.request_body_timeout_ms),
            };
            request_policy.check(request.body())?;
            let options = prost_http::ForwardOptions { request_policy, notify_completion: end_hook };
            let compression = u.and_then(|u|u.http.as_ref()).filter(|p|p.compression_enabled()).map(|_| (request.method().clone(),request.headers().get_all(header::ACCEPT_ENCODING).iter().filter_map(|v|v.to_str().ok()).collect::<Vec<_>>().join(",")));
            let mut outcome = if let Some(files) = static_files {
                Box::pin(files.serve(&request,&l.config.routes[route_index.unwrap()].path_prefix,permit,http_keepalive(snapshot.clone(),info,None),end_hook)).await?
            } else {
            let u=u.context("no matching upstream")?;
            if let Some(policy)=&u.http {policy.prepare_request(&mut request,&u.config,options).await?;}
            let upstream_name = event.as_ref().and_then(|e|e.upstream.as_deref()).or_else(||route_index.map(|i|l.config.routes[i].upstream.as_str())).or(l.config.upstream.as_deref());
            if let Some(audit) = &audit { audit.upstream(upstream_name, None); }
            let selection = u.balancer.select_http(u,peer,&request,header_context.as_ref().map(|c|&c.uri));
            match selection {
            Err(e) => {
                let cached = if matches!(e.downcast_ref::<balance::Rejection>(),Some(balance::Rejection::Unavailable)) {u.http.as_ref().and_then(|p|p.unavailable(&request))} else {None};
                cached.ok_or_else(||u.selection_error(e))?.respond(permit,http_keepalive(snapshot.clone(),info,None),options)
            },
            Ok(selection) => {
            let index=selection.index;
            if let Some(audit) = &audit { audit.upstream(upstream_name,Some(&u.config.endpoints[index].address)); }
            u.observed[index].selected();
            if let Some(ctx)=&mut header_context {ctx.upstream_addr=Some(HeaderValue::from_str(&u.config.endpoints[index].address)?);}
            let forward = async {
                if let Some(dns)=&u.dns {let _=dns.endpoint(&u.config.endpoints[index].address).await;}
                let dynamic=if u.dns.is_some() {Some(u.client.current_dns()?)} else {None};
                let scoped_client = if u.config.send_proxy_v2 { Some(info.proxy_clients.get(u,upstream_name.context("upstream name")?,info.addresses)?) } else { None };
                let client=scoped_client.as_ref().or(dynamic.as_ref()).unwrap_or_else(||u.client.current());
                if let Some(policy)=&u.http {
                    Box::pin(policy.forward(request,&u.targets[index],&u.config,client,permit,http_keepalive(snapshot.clone(),info,selection.lease),options)).await
                } else { prost_http::forward(request,&u.targets[index],&u.config,client,permit,http_keepalive(snapshot.clone(),info,selection.lease),options).await }
            };
            let outcome = match forward.await {
                Ok(v)=>v, Err(e)=>{if prost_http::client_request_failure(&e).is_none() && !prost_http::local_capacity_error(&e) {u.observed[index].error();u.health[index].passive_failure();} return Err(e);}
            };
            if let Some(status)=outcome.origin_status() {
                u.observed[index].response(status);
                if u.failed_status(status) {u.health[index].passive_failure();}
            } else if outcome.origin_transport_failed() {
                u.observed[index].error();u.health[index].passive_failure();
            }
            outcome
            }}
            };
            if let Some(tunnel) = outcome.tunnel.take() {
                let audit = audit.clone();
                if let Some(audit) = &audit { audit.arm_tunnel(); }
                let force = self.force.clone();
                let worker_force=worker_generation.force.clone();
                self.tasks.spawn(worker_generation.tasks.track_future(async move {
                    let mut tunnel = tunnel;
                    tokio::select! {
                        _ = async {tokio::select!{_=force.cancelled()=>{},_=worker_force.cancelled()=>{}}} => { tunnel.abort(); let _ = tunnel.await; if let Some(audit) = &audit { audit.finish(None,"aborted"); } },
                        result = &mut tunnel => { if let Some(audit) = &audit { match result { Ok(report) => audit.finish(report.bytes,if report.error { "error" } else { "complete" }),Err(_) => audit.finish(None,"aborted") } } }
                    }
                }));
            }
            if needs & prost_plugin::NEEDS_UPSTREAM_HEADERS != 0 {if let Some(ctx)=&mut header_context {for p in l.plugins.iter().chain(route_plugins.unwrap_or_default()){p.copy_header_inputs(true,outcome.response.headers(),&mut ctx.upstream_headers);}}}
            if let Some(ctx)=&mut header_context {if let Some(info)=outcome.response.extensions().get::<hyper_util::client::legacy::connect::HttpInfo>(){ctx.upstream_addr=Some(HeaderValue::from_str(&info.remote_addr().to_string())?);}}
            let phase=Phase{hook:"response_headers",peer,status:outcome.response.status().as_u16(),context:header_context.as_ref(),skip_headers:overrides.0};
            invoke_http_plugins(&l.plugins,phase,event.as_mut(),Some(outcome.response.headers_mut())).await?;
            if let Some(plugins) = route_plugins { invoke_http_plugins(plugins,phase.child(),event.as_mut(),Some(outcome.response.headers_mut())).await?; }
            if !l.plugins.is_empty() || route_plugins.is_some_and(|p| !p.is_empty()) {
                let upgraded = outcome.response.status() == 101;
                prost_http::sanitize(outcome.response.headers_mut(),upgraded,false);
            }
            attach_trailers(&mut outcome.response,&l.plugins,route_plugins,overrides.1,header_context.as_ref(),head_request,downstream_version)?;
            if let Some((method,accepted))=&compression { u.unwrap().http.as_ref().unwrap().compress(method,accepted,&mut outcome.response); }
            if let Some(mut event) = event.take() {
                if let Some(completion)=outcome.completion.take() {
                let listener=l.clone();
                let generation=snapshot.clone();
                let p=self.clone();
                let worker_force=worker_generation.force.clone();
                self.tasks.spawn(worker_generation.tasks.track_future(async move {
                    let _generation=generation;
                    tokio::select!{_=p.force.cancelled()=>return,_=worker_force.cancelled()=>return,_=completion=>{}}
                    event.hook="end".into();
                    let result=async {
                        invoke(&listener.plugins,&mut event,None).await?;
                        if let Some(i)=route_index {invoke(&listener.routes[i],&mut event,None).await?;}
                        Ok::<_,anyhow::Error>(())
                    }.await;
                    if let Err(e)=result {p.metrics.errors.inc();tracing::warn!(error=%e,"end hook failed");}
                }));
                }
            }
            Ok::<_,anyhow::Error>(outcome.response)
        }.await;
        match result {
            Ok(response) => response,
            Err(e) => {
                if let Some(failure) = prost_http::client_request_failure(&e)
                    .or_else(|| e.downcast_ref::<prost_http::HttpFailure>().copied())
                {
                    if !failure.is_client() {
                        self.metrics.errors.inc();
                    }
                    let mut response = prost_http::response(failure.status(), failure.message());
                    apply_local_headers(
                        &mut response,
                        &snapshot,
                        &id,
                        selected_route,
                        peer,
                        header_context.as_ref(),
                        (head_request, downstream_version),
                    )
                    .await;
                    return response;
                }
                if let Some(redirect) = e.downcast_ref::<NativeRedirect>() {
                    let mut response = prost_http::response(redirect.0.status, "");
                    response
                        .headers_mut()
                        .insert(header::LOCATION, redirect.0.location.clone());
                    apply_local_headers(
                        &mut response,
                        &snapshot,
                        &id,
                        selected_route,
                        peer,
                        header_context.as_ref(),
                        (head_request, downstream_version),
                    )
                    .await;
                    return response;
                }
                if let Some(rejection) = e.downcast_ref::<FilterRejection>() {
                    let mut response = prost_http::response(
                        rejection.status,
                        "request rejected by native filter\n",
                    );
                    if let Some(seconds) = rejection.retry_after {
                        response.headers_mut().insert(
                            header::RETRY_AFTER,
                            HeaderValue::from_str(&seconds.to_string()).unwrap(),
                        );
                    }
                    apply_local_headers(
                        &mut response,
                        &snapshot,
                        &id,
                        selected_route,
                        peer,
                        header_context.as_ref(),
                        (head_request, downstream_version),
                    )
                    .await;
                    return response;
                }
                self.metrics.errors.inc();
                tracing::warn!(error=%format_args!("{e:#}"),"HTTP proxy error");
                let mut response = prost_http::response(503, "upstream or plugin unavailable\n");
                apply_local_headers(
                    &mut response,
                    &snapshot,
                    &id,
                    selected_route,
                    peer,
                    header_context.as_ref(),
                    (head_request, downstream_version),
                )
                .await;
                response
            }
        }
    }
    fn start_health(self: &Arc<Self>, snapshot: &Arc<Snapshot>) {
        let mut probes = self.probes.lock().unwrap();
        probes.retain(|_, weak| weak.strong_count() > 0);
        for upstream in snapshot.upstreams.values() {
            if !upstream.config.health_check.enabled {
                continue;
            }
            let key = Arc::as_ptr(upstream) as usize;
            if probes.contains_key(&key) {
                continue;
            }
            let weak = Arc::downgrade(upstream);
            probes.insert(key, weak.clone());
            let shutdown = self.shutdown.clone();
            let capacity = self.health_capacity.clone();
            self.tasks.spawn(async move {
                loop {
                    let Some(u)=weak.upgrade() else {break};
                    let interval=Duration::from_millis(u.config.health_interval_ms);
                    let mut jobs = tokio::task::JoinSet::new();
                    for (i,e) in u.config.endpoints.iter().enumerate() {
                        if !e.enabled {continue;}
                        if jobs.len() >= 16 {
                            tokio::select! { _=shutdown.cancelled()=>return, _=jobs.join_next()=>{} }
                        }
                        let u = u.clone();
                        let capacity = capacity.clone();
                        jobs.spawn(async move {
                            let Ok(_permit) = capacity.acquire().await else { return; };
                            let ok=health::probe(&u,i).await;
                            u.health[i].observe(ok,&u.config.health_check);
                        });
                    }
                    while !jobs.is_empty() {
                        tokio::select! { _=shutdown.cancelled()=>return, _=jobs.join_next()=>{} }
                    }
                    drop(u);
                    tokio::select!{_=shutdown.cancelled()=>break,_=tokio::time::sleep(interval)=>{}}
                }
            });
        }
    }
    fn start_metrics(self: &Arc<Self>, socket: TcpListener) {
        let weak = Arc::downgrade(self);
        let cancel = self.shutdown.clone();
        self.tasks.spawn(async move {
            loop {
                let accept = tokio::select!{_=cancel.cancelled()=>break,a=socket.accept()=>a};
                let Ok((io,_))=accept else { continue; };
                let Some(p)=weak.upgrade() else { break; };
                let p2=p.clone();
                p.tasks.spawn(async move {
                    let p3=p2.clone();
                    let service=service_fn(move |request: Request<Incoming>| {
                        let p=p3.clone();
                        async move {
                            use prometheus::Encoder;
                            let response = if request.uri().path() == "/metrics" {
                                let mut buffer=Vec::new();
                                let encoder=prometheus::TextEncoder::new();
                                if encoder.encode(&p.metrics.registry.gather(),&mut buffer).is_ok() {
                                    Response::builder().header(http::header::CONTENT_TYPE,encoder.format_type())
                                        .body(http_body_util::Full::new(hyper::body::Bytes::from(buffer))).unwrap()
                                } else {
                                    Response::builder().status(500).body(http_body_util::Full::new(hyper::body::Bytes::new())).unwrap()
                                }
                            } else { Response::builder().status(404).body(http_body_util::Full::new(hyper::body::Bytes::new())).unwrap() };
                            Ok::<_,Infallible>(response)
                        }
                    });
                    let connection=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(io),service);
                    tokio::select!{_=p2.shutdown.cancelled()=>{},_=tokio::time::timeout(Duration::from_secs(5),connection)=>{}}
                });
            }
        });
    }
    pub async fn shutdown(self: &Arc<Self>) {
        let _guard = self.coordinator.lock().await;
        self.shutdown.cancel();
        let quic_endpoints: Vec<_> = self
            .bindings
            .lock()
            .unwrap()
            .values()
            .filter_map(|binding| {
                if let workers::Masters::Quic(endpoint) = &binding.masters {
                    Some(endpoint.clone())
                } else {
                    None
                }
            })
            .collect();
        self.bindings.lock().unwrap().clear();
        self.tasks.close();
        let drain = match self.snapshot().config.runtime.drain_seconds {
            0 => 30,
            value => value,
        };
        if tokio::time::timeout(Duration::from_secs(drain), self.tasks.wait())
            .await
            .is_err()
        {
            self.force.cancel();
            self.tasks.wait().await;
        }
        // Quinn keeps the UDP socket while closed connections finish their
        // transport drain. Wait for that cleanup before returning to an embedder
        // which may immediately start another proxy on the same address.
        for endpoint in &quic_endpoints {
            endpoint.close(quinn::VarInt::from_u32(0x100), b"proxy stopped");
        }
        drop(quic_endpoints);
        self.quic_background.close();
        if tokio::time::timeout(Duration::from_secs(5), self.quic_background.wait())
            .await
            .is_err()
        {
            tracing::warn!("QUIC transport cleanup timed out");
        }
        let workers = self.workers.lock().unwrap().workers.clone();
        if let Some(workers) = workers {
            let _ = tokio::task::spawn_blocking(move || workers.join()).await;
        }
        if let Some(log) = &self.access_log {
            if let Err(error) = log.shutdown().await {
                tracing::warn!(%error, "access log shutdown incomplete");
            }
        }
    }
}
fn connection_permits(plugins: &[PreparedPlugin]) -> Result<Vec<OwnedSemaphorePermit>> {
    plugins
        .iter()
        .filter_map(|p| p.acquire_connection().transpose())
        .collect()
}
fn http_keepalive(
    snapshot: Arc<Snapshot>,
    info: Arc<ConnectionInfo>,
    lease: Option<balance::Lease>,
) -> prost_http::KeepAlive {
    if let Some(lease) = lease {
        prost_http::KeepAlive::new(Arc::new((snapshot, lease)), info)
    } else {
        prost_http::KeepAlive::new(snapshot, info)
    }
}
fn request_host<B>(request: &Request<B>) -> &str {
    let host = request
        .uri()
        .host()
        .or_else(|| {
            request
                .headers()
                .get(header::HOST)
                .and_then(|v| v.to_str().ok())
        })
        .unwrap_or("");
    if let Some(bracketed) = host.strip_prefix('[') {
        bracketed.split(']').next().unwrap_or(bracketed)
    } else {
        host.split(':').next().unwrap_or(host)
    }
}
fn select_route_index<B>(listener: &PreparedListener, request: &Request<B>) -> Option<usize> {
    if listener.route_order.is_empty() {
        return None;
    }
    let host = request_host(request);
    if listener.route_order.len() > 32 {
        return listener
            .route_index
            .select(&listener.config, host, request.uri().path(), |i| {
                listener.route_matches[i].matches(request.method(), request.headers())
            });
    }
    listener.route_order.iter().copied().find(|i| {
        let route = &listener.config.routes[*i];
        route
            .host
            .as_ref()
            .is_none_or(|h| h.eq_ignore_ascii_case(host))
            && request.uri().path().starts_with(&route.path_prefix)
            && listener.route_matches[*i].matches(request.method(), request.headers())
    })
}
#[derive(Debug)]
struct FilterRejection {
    status: u16,
    retry_after: Option<u64>,
}
impl std::fmt::Display for FilterRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native filter rejected request ({})", self.status)
    }
}
impl std::error::Error for FilterRejection {}

#[derive(Debug)]
struct NativeRedirect(Arc<prost_plugin::Redirect>);
impl std::fmt::Display for NativeRedirect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native redirect ({})", self.0.status)
    }
}
impl std::error::Error for NativeRedirect {}

#[derive(Clone, Copy)]
struct Phase<'a> {
    hook: &'static str,
    peer: std::net::IpAddr,
    status: u16,
    context: Option<&'a HeaderContext>,
    skip_headers: u8,
}
impl Phase<'_> {
    fn new(hook: &'static str, peer: std::net::IpAddr) -> Self {
        Self {
            hook,
            peer,
            status: 200,
            context: None,
            skip_headers: 0,
        }
    }
    fn child(self) -> Self {
        Self {
            skip_headers: 0,
            ..self
        }
    }
}
async fn invoke_http_plugins(
    plugins: &[PreparedPlugin],
    phase: Phase<'_>,
    mut event: Option<&mut Event>,
    mut headers: Option<&mut http::HeaderMap>,
) -> Result<()> {
    for plugin in plugins {
        let hook = phase.hook;
        if !plugin.has_hook(hook) {
            continue;
        }
        if plugin.is_filter() {
            let call = FilterCall {
                hook: match hook {
                    "connection" => FilterHook::Connection,
                    "request_headers" => FilterHook::RequestHeaders,
                    _ => FilterHook::ResponseHeaders,
                },
                peer: phase.peer,
                status: phase.status,
                context: phase.context,
                inherited_skip: if plugin.parent_override().is_some() {
                    phase.skip_headers
                } else {
                    0
                },
            };
            let decision = plugin.filter_call(&call, headers.as_deref_mut())?;
            match decision {
                FilterDecision::Continue => {}
                FilterDecision::Redirect(redirect) => {
                    ensure!(
                        hook == "request_headers"
                            && matches!(redirect.status, 301 | 302 | 303 | 307 | 308),
                        "invalid native redirect"
                    );
                    return Err(NativeRedirect(redirect).into());
                }
                FilterDecision::Reject {
                    status,
                    retry_after,
                } => {
                    ensure!(
                        (400..=599).contains(&status),
                        "invalid native filter rejection status"
                    );
                    return Err(FilterRejection {
                        status,
                        retry_after,
                    }
                    .into());
                }
            }
        } else if let Some(event) = event.as_deref_mut() {
            if event.hook != hook {
                hook.clone_into(&mut event.hook);
            }
            invoke(std::slice::from_ref(plugin), event, headers.as_deref_mut()).await?;
        }
    }
    Ok(())
}
// URI-aware execution is boxed only when enabled, keeping ordinary request
// futures small and preserving the existing header/ACL/rate filter path.
async fn invoke_uri_plugins(
    parent: &[PreparedPlugin],
    child: Option<&[PreparedPlugin]>,
    phase: Phase<'_>,
    mut event: Option<&mut Event>,
    request: &mut Request<IncomingBody>,
) -> (http::Uri, Result<()>) {
    let mut uri = request.uri().clone();
    let result = async {
        for (plugin, phase) in parent
            .iter()
            .map(|p| (p, phase))
            .chain(child.unwrap_or_default().iter().map(|p| (p, phase.child())))
        {
            if !plugin.has_hook("request_headers") {
                continue;
            }
            if !plugin.is_filter() {
                if let Some(event) = event.as_deref_mut() {
                    if event.hook != "request_headers" {
                        event.hook = "request_headers".into();
                    }
                    invoke(
                        std::slice::from_ref(plugin),
                        event,
                        Some(request.headers_mut()),
                    )
                    .await?;
                }
                continue;
            }
            let call = FilterCall {
                hook: FilterHook::RequestHeaders,
                peer: phase.peer,
                status: 200,
                context: phase.context,
                inherited_skip: if plugin.parent_override().is_some() {
                    phase.skip_headers
                } else {
                    0
                },
            };
            match plugin.filter_request(&call, &mut uri, request.headers_mut())? {
                FilterDecision::Continue => {}
                FilterDecision::Redirect(redirect) => {
                    ensure!(
                        matches!(redirect.status, 301 | 302 | 303 | 307 | 308),
                        "invalid native redirect"
                    );
                    return Err(NativeRedirect(redirect).into());
                }
                FilterDecision::Reject {
                    status,
                    retry_after,
                } => {
                    ensure!(
                        (400..=599).contains(&status),
                        "invalid native filter rejection status"
                    );
                    return Err(FilterRejection {
                        status,
                        retry_after,
                    }
                    .into());
                }
            }
        }
        Ok(())
    }
    .await;
    (uri, result)
}
fn attach_trailers(
    response: &mut Response<Body>,
    parent: &[PreparedPlugin],
    child: Option<&[PreparedPlugin]>,
    skip_parent: bool,
    context: Option<&HeaderContext>,
    head: bool,
    version: http::Version,
) -> Result<()> {
    if head
        || version == http::Version::HTTP_10
        || response.status().is_informational()
        || matches!(response.status().as_u16(), 204 | 304)
    {
        return Ok(());
    }
    let mut all = None::<http::HeaderMap>;
    for plugin in parent
        .iter()
        .filter(|_| !skip_parent)
        .chain(child.unwrap_or_default())
    {
        if let Some(headers) =
            plugin.trailers(response.status().as_u16(), context, response.headers())?
        {
            let all = all.get_or_insert_with(http::HeaderMap::new);
            for (n, v) in headers.iter() {
                all.append(n.clone(), v.clone());
            }
        }
    }
    if let Some(trailers) = all {
        if version == http::Version::HTTP_11 {
            let names = trailers
                .keys()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            response.headers_mut().remove(header::CONTENT_LENGTH);
            response
                .headers_mut()
                .insert(header::TRAILER, HeaderValue::from_str(&names)?);
        }
        let body = std::mem::replace(
            response.body_mut(),
            prost_http::response(200, "").into_body(),
        );
        *response.body_mut() = body.with_trailers(trailers);
    }
    Ok(())
}
async fn apply_local_headers(
    response: &mut Response<Body>,
    snapshot: &Snapshot,
    id: &str,
    route: Option<usize>,
    peer: std::net::IpAddr,
    context: Option<&HeaderContext>,
    body: (bool, http::Version),
) {
    let Some(l) = snapshot.listeners.get(id) else {
        return;
    };
    let child = route.map(|i| l.routes[i].as_slice());
    let skip = child
        .and_then(|ps| ps.iter().find_map(PreparedPlugin::parent_override))
        .map_or(0, |v| v.0);
    for (p, inherited_skip) in l
        .plugins
        .iter()
        .map(|p| {
            (
                p,
                if p.parent_override().is_some() {
                    skip
                } else {
                    0
                },
            )
        })
        .chain(child.unwrap_or_default().iter().map(|p| (p, 0)))
    {
        let call = FilterCall {
            hook: FilterHook::ResponseHeaders,
            peer,
            status: response.status().as_u16(),
            context,
            inherited_skip,
        };
        if p.is_filter() && p.has_hook("response_headers") {
            if let Err(e) = p.filter_call(&call, Some(response.headers_mut())) {
                tracing::warn!(error=%e,"local response header filter failed");
            }
        }
    }
    prost_http::sanitize(response.headers_mut(), false, false);
    let skip_trailers = child
        .and_then(|ps| ps.iter().find_map(PreparedPlugin::parent_override))
        .is_some_and(|v| v.1);
    if let Err(e) = attach_trailers(
        response,
        &l.plugins,
        child,
        skip_trailers,
        context,
        body.0,
        body.1,
    ) {
        tracing::warn!(error=%e,"local response trailers failed");
    }
}
async fn invoke(
    plugins: &[PreparedPlugin],
    event: &mut Event,
    mut headers: Option<&mut http::HeaderMap>,
) -> Result<()> {
    for p in plugins {
        if !p.has_event_hooks() || !p.has_hook(&event.hook) {
            continue;
        }
        if let Some(h) = &headers {
            event.headers = h
                .iter()
                .filter_map(|(k, v)| {
                    v.to_str()
                        .ok()
                        .map(|v| (k.as_str().to_string(), v.to_string()))
                })
                .collect();
        }
        let action = p.invoke(event).await?;
        ensure!(!action.deny, "plugin denied event");
        if let Some(upstream) = action.upstream {
            event.upstream = Some(upstream);
        }
        if let Some(h) = headers.as_deref_mut() {
            for (name, value) in action.set_headers {
                h.insert(
                    HeaderName::from_bytes(name.as_bytes())?,
                    HeaderValue::from_str(&value)?,
                );
            }
        }
    }
    Ok(())
}
