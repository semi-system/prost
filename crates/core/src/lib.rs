#![cfg(unix)]
mod storage;

use anyhow::{bail, ensure, Context, Result};
use arc_swap::ArcSwap;
use http::{header, HeaderName, HeaderValue, Request, Response};
use hyper::{body::Incoming, service::service_fn};
use hyper_util::{
    rt::{TokioExecutor, TokioIo, TokioTimer},
    server::conn::auto,
};
use prost_config::{Config, Listener, Protocol, Upstream};
use prost_http::{Body, HttpClient};
use prost_plugin::{Event, PluginRegistry, PreparedPlugin};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    fs::File,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
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

pub struct Metrics {
    pub registry: prometheus::Registry,
    pub connections: prometheus::IntGauge,
    pub requests: prometheus::IntCounter,
    pub errors: prometheus::IntCounter,
    pub bytes: prometheus::IntCounter,
    pub commits: prometheus::IntCounter,
    pub latency: prometheus::Histogram,
}
impl Metrics {
    fn new() -> Result<Self> {
        let registry = prometheus::Registry::new();
        let m = Self {
            registry,
            connections: prometheus::IntGauge::new(
                "prost_connections",
                "Active accepted connections",
            )?,
            requests: prometheus::IntCounter::new("prost_requests_total", "HTTP requests")?,
            errors: prometheus::IntCounter::new("prost_errors_total", "Traffic and plugin errors")?,
            bytes: prometheus::IntCounter::new("prost_stream_bytes_total", "TCP bytes relayed")?,
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
        m.registry.register(Box::new(m.requests.clone()))?;
        m.registry.register(Box::new(m.errors.clone()))?;
        m.registry.register(Box::new(m.bytes.clone()))?;
        m.registry.register(Box::new(m.commits.clone()))?;
        m.registry.register(Box::new(m.latency.clone()))?;
        Ok(m)
    }
}
pub struct Snapshot {
    pub config: Config,
    pub hash: String,
    upstreams: BTreeMap<String, Arc<PreparedUpstream>>,
    listeners: BTreeMap<String, Arc<PreparedListener>>,
}
struct PreparedUpstream {
    config: Upstream,
    tls: Arc<rustls::ClientConfig>,
    client: HttpClient,
    healthy: Vec<AtomicBool>,
    counter: AtomicU64,
    targets: Vec<prost_http::Target>,
}
impl PreparedUpstream {
    fn select(&self) -> Result<(usize, &str)> {
        if self.config.endpoints.len() == 1 {
            let endpoint = &self.config.endpoints[0];
            ensure!(
                endpoint.enabled && self.healthy[0].load(Ordering::Relaxed),
                "no healthy endpoints"
            );
            return Ok((0, &endpoint.address));
        }
        let total: u64 = self
            .config
            .endpoints
            .iter()
            .enumerate()
            .filter(|(i, e)| e.enabled && self.healthy[*i].load(Ordering::Relaxed))
            .map(|(_, e)| e.weight as u64)
            .sum();
        ensure!(total > 0, "no healthy endpoints");
        let mut ticket = self.counter.fetch_add(1, Ordering::Relaxed) % total;
        for (i, e) in self.config.endpoints.iter().enumerate() {
            if e.enabled && self.healthy[i].load(Ordering::Relaxed) {
                if ticket < e.weight as u64 {
                    return Ok((i, &e.address));
                }
                ticket -= e.weight as u64;
            }
        }
        bail!("endpoint health changed during selection")
    }
}
struct PreparedListener {
    config: Listener,
    tls: Option<Arc<rustls::ServerConfig>>,
    capacity: Arc<Semaphore>,
    plugins: Arc<Vec<PreparedPlugin>>,
    routes: Vec<Arc<Vec<PreparedPlugin>>>,
    route_order: Vec<usize>,
    default_upstream: Option<Arc<PreparedUpstream>>,
    route_upstreams: Vec<Arc<PreparedUpstream>>,
    has_hooks: bool,
}
struct ConnectionInfo {
    peer: SocketAddr,
    peer_text: String,
    forwarded_for: HeaderValue,
    forwarded_proto: HeaderValue,
    _lease: ConnectionLease,
}
struct ConnectionLease {
    _global: OwnedSemaphorePermit,
    _local: OwnedSemaphorePermit,
    _plugins: Vec<OwnedSemaphorePermit>,
    active: prometheus::IntGauge,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.active.dec();
    }
}
struct Binding {
    cancel: CancellationToken,
}
pub struct RunningProxy {
    current: ArcSwap<Snapshot>,
    path: PathBuf,
    _lock: File,
    registry: Arc<PluginRegistry>,
    coordinator: AsyncMutex<()>,
    bindings: Mutex<BTreeMap<String, Binding>>,
    connections: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    handshakes: Arc<Semaphore>,
    tasks: TaskTracker,
    shutdown: CancellationToken,
    force: CancellationToken,
    generations: Mutex<Vec<Weak<Snapshot>>>,
    probes: Mutex<BTreeMap<usize, Weak<PreparedUpstream>>>,
    pub metrics: Metrics,
}
pub struct ProxyBuilder {
    path: PathBuf,
    registry: PluginRegistry,
}
impl ProxyBuilder {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            path: path.into(),
            registry: PluginRegistry::new()?,
        })
    }
    pub fn plugins(mut self, registry: PluginRegistry) -> Self {
        self.registry = registry;
        self
    }
    pub async fn start(self) -> Result<Arc<RunningProxy>> {
        let path = std::fs::canonicalize(&self.path).context("config path must exist")?;
        let lock = storage::lock(&path)?;
        let raw = Config::parse(&std::fs::read_to_string(&path)?)?;
        let registry = Arc::new(self.registry);
        let r = registry.clone();
        let snapshot =
            tokio::task::spawn_blocking(move || prepare(storage::stage(raw)?, &r, None)).await??;
        let mut sockets = Vec::new();
        for l in &snapshot.config.listeners {
            sockets.push((l.id.clone(), TcpListener::bind(l.address).await?));
        }
        let metrics = Metrics::new()?;
        let metrics_socket = match snapshot.config.runtime.metrics_address {
            Some(address) => Some(TcpListener::bind(address).await?),
            None => None,
        };
        let encoded = snapshot.config.encode()?;
        let persist_path = path.clone();
        ensure!(
            tokio::task::spawn_blocking(move || storage::atomic_write(
                &persist_path,
                encoded.as_bytes()
            ))
            .await??,
            "initial directory fsync failed"
        );
        let runtime = snapshot.config.runtime.clone();
        let snapshot = Arc::new(snapshot);
        let p = Arc::new(RunningProxy {
            current: ArcSwap::from(snapshot.clone()),
            path,
            _lock: lock,
            registry,
            coordinator: AsyncMutex::new(()),
            bindings: Mutex::new(BTreeMap::new()),
            connections: Arc::new(Semaphore::new(runtime.max_connections)),
            requests: Arc::new(Semaphore::new(runtime.max_requests)),
            handshakes: Arc::new(Semaphore::new(256)),
            tasks: TaskTracker::new(),
            shutdown: CancellationToken::new(),
            force: CancellationToken::new(),
            generations: Mutex::new(vec![Arc::downgrade(&snapshot)]),
            probes: Mutex::new(BTreeMap::new()),
            metrics,
        });
        for (id, socket) in sockets {
            p.start_listener(id, socket);
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
) -> Result<Snapshot> {
    config.validate()?;
    let mut upstreams = BTreeMap::new();
    for (name, u) in &config.upstreams {
        if let Some(old) = previous.and_then(|s| s.upstreams.get(name)) {
            if serde_json::to_value(&old.config)? == serde_json::to_value(u)? {
                upstreams.insert(name.clone(), old.clone());
                continue;
            }
        }
        let tls = prost_transport::client_tls(u.tls.as_ref())?;
        let client = prost_http::client(u, tls.clone())?;
        upstreams.insert(
            name.clone(),
            Arc::new(PreparedUpstream {
                config: u.clone(),
                tls,
                client,
                healthy: u.endpoints.iter().map(|_| AtomicBool::new(true)).collect(),
                counter: AtomicU64::new(0),
                targets: u
                    .endpoints
                    .iter()
                    .map(|e| prost_http::Target::new(&e.address, u.tls.is_some()))
                    .collect::<Result<_>>()?,
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
            if upstreams_unchanged && serde_json::to_value(&old.config)? == serde_json::to_value(l)?
            {
                listeners.insert(l.id.clone(), old.clone());
                continue;
            }
        }
        let capacity = previous_l
            .filter(|p| p.config.max_connections == l.max_connections)
            .map(|p| p.capacity.clone())
            .unwrap_or_else(|| Arc::new(Semaphore::new(l.max_connections)));
        let tls = l
            .tls
            .as_ref()
            .map(|t| prost_transport::server_tls(t, l.protocol == Protocol::Http))
            .transpose()?;
        let plugins = if let Some(old) = previous_l.filter(|old| {
            serde_json::to_value(&old.config.plugins).ok() == serde_json::to_value(&l.plugins).ok()
        }) {
            old.plugins.clone()
        } else {
            Arc::new(
                l.plugins
                    .iter()
                    .map(|p| registry.prepare(p))
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        let has_hooks = plugins.iter().any(PreparedPlugin::has_hooks);
        let routes = l
            .routes
            .iter()
            .enumerate()
            .map(|(i, r)| {
                if let Some(old) = previous_l.filter(|old| {
                    old.config.routes.get(i).is_some_and(|old_r| {
                        serde_json::to_value(&old_r.plugins).ok()
                            == serde_json::to_value(&r.plugins).ok()
                    })
                }) {
                    return Ok(old.routes[i].clone());
                }
                Ok(Arc::new(
                    r.plugins
                        .iter()
                        .map(|p| registry.prepare(p))
                        .collect::<Result<Vec<_>>>()?,
                ))
            })
            .collect::<Result<_>>()?;
        let mut route_order: Vec<usize> = (0..l.routes.len()).collect();
        route_order.sort_by_key(|i| {
            (
                std::cmp::Reverse(l.routes[*i].priority),
                std::cmp::Reverse(l.routes[*i].path_prefix.len()),
                *i,
            )
        });
        let default_upstream = l.upstream.as_ref().map(|name| upstreams[name].clone());
        let route_upstreams = l
            .routes
            .iter()
            .map(|r| upstreams[&r.upstream].clone())
            .collect();
        listeners.insert(
            l.id.clone(),
            Arc::new(PreparedListener {
                config: l.clone(),
                tls,
                capacity,
                plugins,
                routes,
                route_order,
                default_upstream,
                route_upstreams,
                has_hooks,
            }),
        );
    }
    let hash = storage::hash(config.encode()?.as_bytes());
    Ok(Snapshot {
        config,
        hash,
        upstreams,
        listeners,
    })
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
        serde_json::json!({"generation": snapshot.config.generation, "hash": snapshot.hash, "live_generations": generations.len(), "connections": self.metrics.connections.get(), "shutting_down": self.shutdown.is_cancelled()})
    }
    pub async fn validate(self: &Arc<Self>, text: String) -> Result<()> {
        let _guard = self.coordinator.lock().await;
        let old = self.snapshot();
        let config = Config::parse(&text)?;
        self.compatible(&config, &old)?;
        let registry = self.registry.clone();
        tokio::task::spawn_blocking(move || {
            prepare(storage::stage(config)?, &registry, Some(&old))
        })
        .await??;
        Ok(())
    }
    fn compatible(&self, config: &Config, old: &Snapshot) -> Result<()> {
        ensure!(!self.shutdown.is_cancelled(), "proxy shutting down");
        ensure!(
            config.runtime == old.config.runtime,
            "runtime settings require restart"
        );
        for l in &config.listeners {
            for previous in &old.config.listeners {
                if previous.address == l.address {
                    ensure!(
                        previous.id == l.id && previous.protocol == l.protocol,
                        "listener identity/protocol change on same address requires restart"
                    );
                }
            }
            if let Some(previous) = old.listeners.get(&l.id) {
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
        tokio::spawn(async move { p.commit(text, expected).await }).await?
    }
    pub async fn set_enabled(
        self: &Arc<Self>,
        endpoint: String,
        enabled: bool,
        expected: u64,
    ) -> Result<serde_json::Value> {
        let p = self.clone();
        tokio::spawn(async move {
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
        let new = Arc::new(
            tokio::task::spawn_blocking(move || {
                prepare(storage::stage(config)?, &registry, Some(&previous))
            })
            .await??,
        );
        let mut sockets = Vec::new();
        for l in &new.config.listeners {
            if !old.listeners.contains_key(&l.id) {
                sockets.push((
                    l.id.clone(),
                    TcpListener::bind(l.address)
                        .await
                        .with_context(|| format!("bind {}", l.address))?,
                ));
            }
        }
        let path = self.path.clone();
        let encoded = new.config.encode()?;
        let durable =
            tokio::task::spawn_blocking(move || storage::atomic_write(&path, encoded.as_bytes()))
                .await??;
        // No fallible preparation past this point.
        self.current.store(new.clone());
        self.generations.lock().unwrap().push(Arc::downgrade(&new));
        self.bindings.lock().unwrap().retain(|id, b| {
            if !new.listeners.contains_key(id) {
                b.cancel.cancel();
                false
            } else {
                true
            }
        });
        for (id, socket) in sockets {
            self.start_listener(id, socket);
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
    fn start_listener(self: &Arc<Self>, id: String, socket: TcpListener) {
        let cancel = self.shutdown.child_token();
        self.bindings.lock().unwrap().insert(
            id.clone(),
            Binding {
                cancel: cancel.clone(),
            },
        );
        let weak = Arc::downgrade(self);
        self.tasks.spawn(async move {
            loop {
                let accepted = tokio::select! { biased; _ = cancel.cancelled() => break, a = socket.accept() => a };
                let (stream,peer) = match accepted { Ok(v) => v, Err(e) => { tracing::warn!(error=%e,"accept failed"); tokio::time::sleep(Duration::from_millis(100)).await; continue; } };
                let Some(p) = weak.upgrade() else { break; };
                let snapshot = p.snapshot();
                let Some(listener) = snapshot.listeners.get(&id).cloned() else { continue; };
                let global = match p.connections.clone().try_acquire_owned() { Ok(v)=>v,Err(_)=>continue };
                let local = match listener.capacity.clone().try_acquire_owned() { Ok(v)=>v,Err(_)=>continue };
                let plugin_permits = match connection_permits(&listener.plugins) { Ok(v)=>v,Err(_)=>continue };
                p.metrics.connections.inc();
                let lease = ConnectionLease { _global:global, _local:local, _plugins:plugin_permits, active:p.metrics.connections.clone() };
                let p2 = p.clone();
                let id = id.clone();
                p.tasks.spawn(async move {
                    let result = tokio::select! {
                        _ = p2.force.cancelled() => Ok(()),
                        r = p2.connection(id,stream,peer,snapshot,listener,lease) => r,
                    };
                    if let Err(e) = result { p2.metrics.errors.inc(); tracing::debug!(error=%e,%peer,"connection ended with error"); }
                });
            }
        });
    }
    async fn connection(
        self: &Arc<Self>,
        id: String,
        stream: TcpStream,
        peer: SocketAddr,
        snapshot: Arc<Snapshot>,
        listener: Arc<PreparedListener>,
        lease: ConnectionLease,
    ) -> Result<()> {
        prost_transport::configure_socket(&stream)?;
        let mut io: prost_transport::BoxStream = Box::new(stream);
        if let Some(tls) = &listener.tls {
            let _permit = self
                .handshakes
                .clone()
                .try_acquire_owned()
                .context("handshake capacity")?;
            io = Box::new(
                tokio::time::timeout(
                    Duration::from_secs(10),
                    tokio_rustls::TlsAcceptor::from(tls.clone()).accept(io),
                )
                .await??,
            );
        }
        let mut event = Event {
            abi_version: 1,
            hook: "connection".into(),
            peer: peer.to_string(),
            upstream: listener.config.upstream.clone(),
            headers: BTreeMap::new(),
            config: serde_json::Value::Null,
        };
        if listener.has_hooks {
            invoke(&listener.plugins, &mut event, None).await?;
        }
        if listener.config.protocol == Protocol::Tcp {
            event.hook = "upstream".into();
            invoke(&listener.plugins, &mut event, None).await?;
            let u = snapshot
                .upstreams
                .get(event.upstream.as_ref().context("no upstream selected")?)
                .context("plugin selected unknown upstream")?;
            let (index, address) = u.select()?;
            let backend = match prost_transport::connect(address, &u.config, u.tls.clone()).await {
                Ok(v) => v,
                Err(e) => {
                    u.healthy[index].store(false, Ordering::Relaxed);
                    return Err(e);
                }
            };
            let result = prost_transport::relay(
                io,
                backend,
                Duration::from_millis(u.config.idle_timeout_ms),
            )
            .await;
            if let Ok(bytes) = &result {
                self.metrics.bytes.inc_by(*bytes);
            }
            event.hook = "end".into();
            invoke(&listener.plugins, &mut event, None).await?;
            return result.map(|_| ());
        }
        let info = Arc::new(ConnectionInfo {
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
        drop(snapshot);
        drop(listener);
        let p = self.clone();
        let service = service_fn(move |request| {
            let p = p.clone();
            let id = id.clone();
            let info = info.clone();
            async move { Ok::<_, Infallible>(p.handle_http(id, info, request).await) }
        });
        let mut builder = auto::Builder::new(TokioExecutor::new());
        builder
            .http1()
            .timer(TokioTimer::new())
            .header_read_timeout(Duration::from_secs(10))
            .max_buf_size(32768);
        builder
            .http2()
            .timer(TokioTimer::new())
            .max_concurrent_streams(128)
            .max_header_list_size(32768)
            .max_send_buf_size(32768);
        let connection = builder.serve_connection_with_upgrades(TokioIo::new(io), service);
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => { result.map_err(|e| anyhow::anyhow!(e))?; },
            _ = self.shutdown.cancelled() => { connection.as_mut().graceful_shutdown(); connection.await.map_err(|e| anyhow::anyhow!(e))?; },
        }
        Ok(())
    }
    async fn handle_http(
        self: Arc<Self>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        mut request: Request<Incoming>,
    ) -> Response<Body> {
        self.metrics.requests.inc();
        let _timer = self.metrics.latency.start_timer();
        let snapshot = self.snapshot();
        let result = async {
            let permit = self.requests.clone().try_acquire_owned().context("request capacity")?;
            let l = snapshot.listeners.get(id.as_ref()).context("listener removed")?;
            let route_index = if l.route_order.is_empty() { None } else {
                let host = request_host(&request);
                l.route_order.iter().copied().find(|i| {
                    let r = &l.config.routes[*i];
                    r.host.as_ref().is_none_or(|h| h.eq_ignore_ascii_case(host)) && request.uri().path().starts_with(&r.path_prefix)
                })
            };
            let selected = route_index.map(|i| &l.route_upstreams[i]).or(l.default_upstream.as_ref());
            let Some(selected) = selected else { return Ok(prost_http::response(404,"no matching route\n")); };
            if !l.config.trusted_proxies.iter().any(|cidr| cidr.contains(&info.peer.ip())) {
                let names: Vec<_> = request.headers().keys().filter(|n| n.as_str() == "forwarded" || n.as_str().starts_with("x-forwarded-")).cloned().collect();
                for name in names { request.headers_mut().remove(name); }
            }
            request.headers_mut().append(FORWARDED_FOR.clone(),info.forwarded_for.clone());
            request.headers_mut().insert(FORWARDED_PROTO.clone(),info.forwarded_proto.clone());
            let route_plugins = route_index.map(|i|l.routes[i].as_slice());
            let has_hooks = l.has_hooks || route_plugins.is_some_and(|plugins| plugins.iter().any(PreparedPlugin::has_hooks));
            let mut event = if has_hooks {
                let upstream = route_index.map(|i|l.config.routes[i].upstream.clone()).or_else(||l.config.upstream.clone());
                let mut event = Event { abi_version:1, hook:"request_headers".into(), peer:info.peer_text.clone(), upstream, headers:BTreeMap::new(),config:serde_json::Value::Null };
                invoke(&l.plugins,&mut event,Some(request.headers_mut())).await?;
                if let Some(plugins) = route_plugins { invoke(plugins,&mut event,Some(request.headers_mut())).await?; }
                event.hook = "upstream".into();
                invoke(&l.plugins,&mut event,None).await?;
                if let Some(plugins) = route_plugins { invoke(plugins,&mut event,None).await?; }
                Some(event)
            } else { None };
            let u = if let Some(event) = &event {
                snapshot.upstreams.get(event.upstream.as_ref().context("no upstream")?).context("plugin selected unknown upstream")?
            } else { selected };
            let (index,_) = u.select()?;
            let mut outcome = match prost_http::forward(request,&u.targets[index],&u.config,&u.client,permit,prost_http::KeepAlive::new(snapshot.clone(),info),has_hooks).await {
                Ok(v)=>v, Err(e)=>{u.healthy[index].store(false,Ordering::Relaxed); return Err(e);}
            };
            if let Some(tunnel) = outcome.tunnel.take() {
                let force = self.force.clone();
                self.tasks.spawn(async move {
                    let mut tunnel = tunnel;
                    tokio::select! { _ = force.cancelled() => { tunnel.abort(); let _ = tunnel.await; }, _ = &mut tunnel => {} }
                });
            }
            if let Some(mut event) = event.take() {
                event.hook = "response_headers".into();
                invoke(&l.plugins,&mut event,Some(outcome.response.headers_mut())).await?;
                if let Some(plugins) = route_plugins { invoke(plugins,&mut event,Some(outcome.response.headers_mut())).await?; }
                let upgraded = outcome.response.status() == 101;
                prost_http::sanitize(outcome.response.headers_mut(),upgraded,false);
                let completion=outcome.completion.take().expect("plugin completion enabled");
                let listener=l.clone();
                let generation=snapshot.clone();
                let p=self.clone();
                self.tasks.spawn(async move {
                    let _generation=generation;
                    tokio::select!{_=p.force.cancelled()=>return,_=completion=>{}}
                    event.hook="end".into();
                    let result=async {
                        invoke(&listener.plugins,&mut event,None).await?;
                        if let Some(i)=route_index {invoke(&listener.routes[i],&mut event,None).await?;}
                        Ok::<_,anyhow::Error>(())
                    }.await;
                    if let Err(e)=result {p.metrics.errors.inc();tracing::warn!(error=%e,"end hook failed");}
                });
            }
            Ok::<_,anyhow::Error>(outcome.response)
        }.await;
        match result {
            Ok(response) => response,
            Err(e) => {
                self.metrics.errors.inc();
                tracing::warn!(error=%e,"HTTP proxy error");
                prost_http::response(503, "upstream or plugin unavailable\n")
            }
        }
    }
    fn start_health(self: &Arc<Self>, snapshot: &Arc<Snapshot>) {
        let mut probes = self.probes.lock().unwrap();
        probes.retain(|_, weak| weak.strong_count() > 0);
        for upstream in snapshot.upstreams.values() {
            let key = Arc::as_ptr(upstream) as usize;
            if probes.contains_key(&key) {
                continue;
            }
            let weak = Arc::downgrade(upstream);
            probes.insert(key, weak.clone());
            let shutdown = self.shutdown.clone();
            self.tasks.spawn(async move {
                loop {
                    let Some(u)=weak.upgrade() else {break};
                    let interval=Duration::from_millis(u.config.health_interval_ms);
                    for (i,e) in u.config.endpoints.iter().enumerate() {
                        if !e.enabled {continue;}
                        let ok=tokio::select!{_=shutdown.cancelled()=>return,r=prost_transport::connect(&e.address,&u.config,u.tls.clone())=>r.is_ok()};
                        u.healthy[i].store(ok,Ordering::Relaxed);
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
                                let _=prometheus::TextEncoder::new().encode(&p.metrics.registry.gather(),&mut buffer);
                                let text=String::from_utf8_lossy(&buffer).to_string();
                                Response::new(http_body_util::Full::new(hyper::body::Bytes::from(text)))
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
        self.tasks.close();
        let drain = self.snapshot().config.runtime.drain_seconds;
        if tokio::time::timeout(Duration::from_secs(drain), self.tasks.wait())
            .await
            .is_err()
        {
            self.force.cancel();
            self.tasks.wait().await;
        }
    }
}
fn connection_permits(plugins: &[PreparedPlugin]) -> Result<Vec<OwnedSemaphorePermit>> {
    plugins
        .iter()
        .filter_map(|p| p.acquire_connection().transpose())
        .collect()
}
fn request_host(request: &Request<Incoming>) -> &str {
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
async fn invoke(
    plugins: &[PreparedPlugin],
    event: &mut Event,
    mut headers: Option<&mut http::HeaderMap>,
) -> Result<()> {
    for p in plugins {
        if !p.has_hooks() {
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
