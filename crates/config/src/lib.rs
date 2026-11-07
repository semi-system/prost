use anyhow::{ensure, Context, Result};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
};

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
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    pub endpoints: Vec<Endpoint>,
    #[serde(default = "balance")]
    pub balance: String,
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
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTls {
    pub server_name: String,
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerTls {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    pub name: String,
    #[serde(default)]
    pub module: Option<PathBuf>,
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
            ensure!(u.balance == "round_robin", "unsupported balance algorithm");
            ensure!(
                !u.endpoints.is_empty() && u.endpoints.len() <= 256,
                "upstream {name} requires 1..256 endpoints"
            );
            ensure!(
                u.connect_timeout_ms > 0 && u.idle_timeout_ms > 0 && u.health_interval_ms >= 100,
                "invalid timeouts"
            );
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
            if let Some(tls) = &u.tls {
                ensure!(!tls.server_name.is_empty(), "TLS server_name is required");
            }
        }
        for l in &self.listeners {
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
                    l.upstream.is_some() && l.routes.is_empty(),
                    "TCP requires upstream and no routes"
                );
            }
            if let Some(u) = &l.upstream {
                ensure!(self.upstreams.contains_key(u), "unknown upstream {u}");
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
        if p.name == "connection_limit" {
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
