use anyhow::{ensure, Context, Result};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
};

mod tls;
pub use tls::{NamedCertificate, ServerTls};

fn yes() -> bool {
    true
}
fn one() -> u32 {
    1
}
fn max_conn() -> usize {
    10_000
}
fn max_req() -> usize {
    10_000
}
fn connect_ms() -> u64 {
    5_000
}
fn idle_ms() -> u64 {
    60_000
}
fn health_ms() -> u64 {
    5_000
}
fn idle_pool() -> usize {
    128
}
fn pool_timeout_ms() -> u64 {
    30_000
}
fn http1_buffer() -> usize {
    64 * 1024
}
fn socket() -> PathBuf {
    "/run/prost/admin.sock".into()
}
fn artifacts() -> PathBuf {
    "/var/lib/prost/artifacts".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema_version: u32,
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub runtime: Runtime,
    pub upstreams: BTreeMap<String, Upstream>,
    pub listeners: Vec<Listener>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Runtime {
    pub admin_socket: PathBuf,
    pub artifact_dir: PathBuf,
    pub metrics_address: Option<SocketAddr>,
    pub max_connections: usize,
    pub max_requests: usize,
    pub drain_seconds: u64,
    pub worker_local: bool,
    pub io_workers: usize,
}
impl Default for Runtime {
    fn default() -> Self {
        Self {
            admin_socket: socket(),
            artifact_dir: artifacts(),
            metrics_address: None,
            max_connections: max_conn(),
            max_requests: max_req(),
            drain_seconds: 30,
            worker_local: true,
            io_workers: 0,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub endpoints: Vec<Endpoint>,
    #[serde(default = "balance")]
    pub balance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash_replicas: Option<u32>,
    #[serde(default)]
    pub tls: Option<ClientTls>,
    #[serde(default = "connect_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "idle_ms")]
    pub idle_timeout_ms: u64,
    #[serde(default = "health_ms")]
    pub health_interval_ms: u64,
    #[serde(default)]
    pub http2: bool,
    #[serde(default = "idle_pool")]
    pub pool_max_idle_per_host: usize,
    #[serde(default = "pool_timeout_ms")]
    pub pool_idle_timeout_ms: u64,
    #[serde(default = "http1_buffer")]
    pub http1_max_buf_size: usize,
    #[serde(default)]
    pub health_check: HealthCheck,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passive_health: Option<PassiveHealth>,
    #[serde(default)]
    pub http2_settings: Http2Settings,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthCheck {
    /// None checks TCP/TLS connectivity; Some performs GET and checks response status.
    pub path: Option<String>,
    pub host: Option<String>,
    pub expected_status: u16,
    pub timeout_ms: u64,
    pub fall: u32,
    pub rise: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PassiveHealth {
    pub max_fails: u32,
    pub fail_timeout_ms: u64,
    pub failure_statuses: Vec<u16>,
}
impl Default for PassiveHealth {
    fn default() -> Self {
        Self {
            max_fails: 1,
            fail_timeout_ms: 10000,
            failure_statuses: Vec::new(),
        }
    }
}
impl Default for HealthCheck {
    fn default() -> Self {
        Self {
            path: None,
            host: None,
            expected_status: 200,
            timeout_ms: 5000,
            fall: 1,
            rise: 1,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Http2Settings {
    pub initial_stream_window_size: u32,
    pub initial_connection_window_size: u32,
    pub max_concurrent_streams: u32,
    pub adaptive_window: bool,
}
impl Default for Http2Settings {
    fn default() -> Self {
        Self {
            initial_stream_window_size: 65535,
            initial_connection_window_size: 1048576,
            max_concurrent_streams: 128,
            adaptive_window: false,
        }
    }
}
impl Http2Settings {
    fn validate(&self) -> Result<()> {
        ensure!(
            (65535..=16777216).contains(&self.initial_stream_window_size)
                && (65535..=16777216).contains(&self.initial_connection_window_size)
                && (1..=4096).contains(&self.max_concurrent_streams),
            "invalid HTTP/2 settings"
        );
        Ok(())
    }
}
fn balance() -> String {
    "round_robin".into()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub id: String,
    pub address: String,
    #[serde(default = "one")]
    pub weight: u32,
    #[serde(default = "yes")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub backup: bool,
    /// Maximum active HTTP streams or TCP sessions; zero means unlimited.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub max_active: u64,
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    pub server_name: String,
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Tcp,
    Http,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    pub id: String,
    pub address: SocketAddr,
    pub protocol: Protocol,
    #[serde(default)]
    pub upstream: Option<String>,
    #[serde(default)]
    pub tls: Option<ServerTls>,
    #[serde(default = "max_conn")]
    pub max_connections: usize,
    #[serde(default)]
    pub trusted_proxies: Vec<IpNet>,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default)]
    pub http2_settings: Http2Settings,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "root")]
    pub path_prefix: String,
    #[serde(default)]
    pub priority: i32,
    pub upstream: String,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
}
fn root() -> String {
    "/".into()
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    pub name: String,
    #[serde(default)]
    pub module: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hooks: Option<Vec<String>>,
    #[serde(default)]
    pub config: BTreeMap<String, toml::Value>,
}
impl Config {
    pub fn parse(text: &str) -> Result<Self> {
        ensure!(text.len() <= 3 * 1024 * 1024, "config exceeds 3 MiB");
        let c: Self = toml::from_str(text).context("invalid TOML")?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.schema_version == 1, "unsupported schema_version");
        ensure!(
            (1..=1_000_000).contains(&self.runtime.max_connections)
                && (1..=1_000_000).contains(&self.runtime.max_requests),
            "runtime limits must be 1..1000000"
        );
        ensure!(
            self.listeners.len() <= 128 && self.upstreams.len() <= 128,
            "too many listeners/upstreams"
        );
        ensure!(
            self.runtime.drain_seconds > 0 && self.runtime.drain_seconds <= 3600,
            "invalid drain_seconds"
        );
        ensure!(self.runtime.io_workers <= 32, "io_workers must be 0..32");
        ensure!(
            self.runtime.admin_socket.is_absolute() && self.runtime.artifact_dir.is_absolute(),
            "runtime paths must be absolute"
        );
        if let Some(a) = self.runtime.metrics_address {
            ensure!(a.ip().is_loopback(), "metrics must bind loopback");
        }
        let mut ids = HashSet::new();
        let mut addresses = HashSet::new();
        for (name, u) in &self.upstreams {
            valid_id(name)?;
            ensure!(
                matches!(
                    u.balance.as_str(),
                    "round_robin" | "least_conn" | "consistent_hash"
                ),
                "unsupported balance algorithm"
            );
            if u.balance != "consistent_hash" {
                ensure!(
                    u.hash_key.is_none() && u.hash_replicas.is_none(),
                    "hash settings require consistent_hash"
                );
            }
            ensure!(
                !u.endpoints.is_empty() && u.endpoints.len() <= 256,
                "upstream {name} requires 1..256 endpoints"
            );
            ensure!(
                u.connect_timeout_ms > 0 && u.idle_timeout_ms > 0 && u.health_interval_ms >= 100,
                "invalid timeouts"
            );
            ensure!(
                u.pool_max_idle_per_host <= 4096 && u.pool_idle_timeout_ms > 0,
                "invalid upstream pool limits"
            );
            ensure!(
                (8192..=1024 * 1024).contains(&u.http1_max_buf_size),
                "HTTP/1 upstream buffer must be 8192..1048576 bytes"
            );
            u.http2_settings.validate()?;
            if let Some(p) = &u.passive_health {
                ensure!(
                    p.max_fails <= 1000 && (1..=3600000).contains(&p.fail_timeout_ms),
                    "invalid passive health threshold/timeout"
                );
                let unique: HashSet<_> = p.failure_statuses.iter().copied().collect();
                ensure!(
                    p.failure_statuses.len() <= 32
                        && unique.len() == p.failure_statuses.len()
                        && p.failure_statuses.iter().all(|s| (400..=599).contains(s)),
                    "passive failure_statuses must contain at most 32 unique HTTP 400..599 codes"
                );
            }
            let h = &u.health_check;
            ensure!(
                (1..=100).contains(&h.fall)
                    && (1..=100).contains(&h.rise)
                    && (1..=60000).contains(&h.timeout_ms)
                    && (200..=599).contains(&h.expected_status),
                "invalid health check policy"
            );
            if let Some(path) = &h.path {
                ensure!(
                    path.starts_with('/')
                        && path.len() <= 4096
                        && !path.bytes().any(|b| b <= 32 || b == 127),
                    "invalid health check path"
                );
            }
            if let Some(host) = &h.host {
                ensure!(
                    !host.is_empty()
                        && host.len() <= 255
                        && !host.bytes().any(|b| b <= 32 || b >= 127),
                    "invalid health check host"
                );
            }
            let mut names = HashSet::new();
            let mut sum = 0u64;
            for e in &u.endpoints {
                valid_id(&e.id)?;
                ensure!(names.insert(&e.id), "duplicate endpoint {}", e.id);
                let (host, port) = e
                    .address
                    .rsplit_once(':')
                    .context("endpoint must be host:port")?;
                ensure!(
                    !host.is_empty() && port.parse::<u16>().unwrap_or(0) > 0,
                    "invalid endpoint address"
                );
                ensure!(
                    !host.contains(['/', '@', '?', '#', ' ']),
                    "invalid endpoint host"
                );
                ensure!(e.weight > 0 && e.weight <= 10000, "weight out of range");
                sum += e.weight as u64;
            }
            ensure!(sum <= 1_000_000, "total weight too large");
            if u.balance == "consistent_hash" {
                ensure!(
                    !u.endpoints.iter().any(|e| e.backup),
                    "backup endpoints require round_robin or least_conn"
                );
                let replicas = u.hash_replicas.unwrap_or(160);
                ensure!(
                    (16..=1024).contains(&replicas),
                    "hash_replicas must be 16..1024"
                );
                ensure!(sum * u64::from(replicas) <= 65536, "consistent hash ring exceeds 65536 points; normalize weights or reduce hash_replicas");
            }
            if let Some(tls) = &u.tls {
                ensure!(!tls.server_name.is_empty(), "TLS server_name is required");
            }
        }
        for l in &self.listeners {
            l.http2_settings.validate()?;
            if let Some(tls) = &l.tls {
                tls.validate()?;
            }
            valid_id(&l.id)?;
            ensure!(ids.insert(&l.id), "duplicate listener id");
            ensure!(
                l.address.port() > 0 && addresses.insert(l.address),
                "duplicate or zero listener address"
            );
            ensure!(l.max_connections > 0, "listener limit must be positive");
            ensure!(
                l.max_connections <= 1_000_000 && l.routes.len() <= 4096,
                "listener limits exceeded"
            );
            if l.protocol == Protocol::Tcp {
                ensure!(
                    !l.plugins
                        .iter()
                        .any(|p| matches!(p.name.as_str(), "headers" | "rate_limit" | "rewrite")),
                    "HTTP filter on TCP listener"
                );
                ensure!(
                    l.upstream.is_some() && l.routes.is_empty(),
                    "TCP requires upstream and no routes"
                );
            }
            if let Some(u) = &l.upstream {
                ensure!(self.upstreams.contains_key(u), "unknown upstream {u}");
                if l.protocol == Protocol::Tcp {
                    let upstream = &self.upstreams[u];
                    ensure!(
                        upstream.balance != "consistent_hash"
                            || upstream.hash_key.as_deref().unwrap_or("$remote_addr")
                                == "$remote_addr",
                        "TCP consistent hash requires $remote_addr"
                    );
                }
            }
            for r in &l.routes {
                ensure!(
                    r.path_prefix.starts_with('/'),
                    "path_prefix must start with /"
                );
                ensure!(
                    self.upstreams.contains_key(&r.upstream),
                    "unknown upstream {}",
                    r.upstream
                );
                validate_plugins(&r.plugins)?;
                ensure!(
                    !r.plugins.iter().any(|p| p.name == "connection_limit"),
                    "connection_limit belongs on listener, not route"
                );
            }
            validate_plugins(&l.plugins)?;
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }
}
fn valid_id(s: &str) -> Result<()> {
    ensure!(
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b)),
        "invalid id {s}"
    );
    Ok(())
}
fn validate_plugins(plugins: &[Plugin]) -> Result<()> {
    ensure!(plugins.len() <= 64, "too many plugins");
    for p in plugins {
        if let Some(hooks) = &p.hooks {
            let mut seen = HashSet::new();
            ensure!(
                hooks.len() <= 5
                    && hooks.iter().all(|hook| matches!(
                        hook.as_str(),
                        "connection" | "upstream" | "request_headers" | "response_headers" | "end"
                    ) && seen.insert(hook)),
                "invalid or duplicate plugin hook"
            );
        }
        if p.name == "connection_limit" {
            ensure!(
                p.hooks.is_none(),
                "connection_limit does not accept hook filters"
            );
            ensure!(p.module.is_none(), "native plugin cannot have module");
            let n = p
                .config
                .get("max_connections")
                .and_then(toml::Value::as_integer)
                .unwrap_or(0);
            ensure!(n > 0 && n <= 1_000_000, "invalid plugin connection limit");
        } else if p.name == "wasm" {
            ensure!(p.module.is_some(), "WASM module required");
        } else {
            valid_id(&p.name)?;
            ensure!(p.module.is_none(), "native plugin cannot specify module");
        }
    }
    Ok(())
}
