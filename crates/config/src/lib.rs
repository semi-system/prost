use anyhow::{ensure, Context, Result};
use ipnet::IpNet;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
};

mod dns;
mod tls_source;
pub use tls_source::{S3Config, S3Object, TlsSource};
#[cfg(test)]
mod encoding_tests;
mod http_policy;
pub use dns::DnsPolicy;
mod http3;
mod logging;
mod route_match;
mod static_files;
mod udp;
pub use http3::Http3Settings;
pub use http_policy::{Cache, Compression, HttpPolicy, Retry};
pub use route_match::RouteHeader;
pub use static_files::StaticFiles;
pub use udp::UdpSettings;
mod tls;
pub use logging::{AccessLog, LogFormat, LogSink};
pub use tls::{
    NamedCertificate, PassthroughRoute, PassthroughSettings, ServerTls, TlsMinimum, TlsPolicy,
};
pub const MAX_CONFIG_BYTES: usize = 3 * 1024 * 1024;

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
    pub max_quic_receive_bytes: usize,
    pub max_udp_buffer_bytes: usize,
    pub max_static_ram_bytes: u64,
    pub reload_mode: ReloadMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3Config>,
    pub max_proxy_connections: usize,
    pub observe_plugins: bool,
    pub access_log: Option<AccessLog>,
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
            max_quic_receive_bytes: 64 * 1024 * 1024,
            max_udp_buffer_bytes: 64 * 1024 * 1024,
            max_static_ram_bytes: 256 * 1024 * 1024,
            reload_mode: ReloadMode::Rolling,
            s3: None,
            max_proxy_connections: 4096,
            observe_plugins: false,
            access_log: None,
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
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadMode {
    #[default]
    Rolling,
    Hot,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dns: Option<DnsPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_retry: Option<TcpRetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub half_close_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub send_proxy_v2: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub observe_pool_state: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<HttpPolicy>,
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
    /// None retains the legacy total request-to-response-header idle timeout.
    /// Some starts a deadline after upload EOF; zero disables that deadline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_header_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_timeout_ms: Option<u64>,
    /// Counts first/reused responses on actual transports, not pool hit/miss.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub observe_connection_reuse: bool,
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
pub struct TcpRetry {
    pub max_attempts: u32,
    pub timeout_ms: u64,
    pub backoff_ms: u64,
}
impl Default for TcpRetry {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            timeout_ms: 9000,
            backoff_ms: 10,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthCheck {
    pub enabled: bool,
    pub protocol: HealthProtocol,
    pub port: Option<u16>,
    pub tls: Option<ClientTls>,
    pub send_proxy_v2: Option<bool>,
    pub proxy_v2_command: ProxyV2Command,
    /// None checks TCP/TLS connectivity; Some performs GET and checks response status.
    pub path: Option<String>,
    pub host: Option<String>,
    pub expected_status: u16,
    pub timeout_ms: u64,
    pub fall: u32,
    pub rise: u32,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProxyV2Command {
    #[default]
    Proxy,
    Local,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthProtocol {
    #[default]
    Connect,
    Http,
    Smtp,
    SmtpStarttls,
    Imap,
    ImapStarttls,
    Pop3,
    Pop3Starttls,
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
            enabled: true,
            protocol: HealthProtocol::Connect,
            port: None,
            tls: None,
            send_proxy_v2: None,
            proxy_v2_command: ProxyV2Command::Proxy,
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
    pub header_read_timeout_ms: u64,
    pub stream_write_timeout_ms: u64,
    pub initial_stream_window_size: u32,
    pub initial_connection_window_size: u32,
    pub max_concurrent_streams: u32,
    pub adaptive_window: bool,
}
impl Default for Http2Settings {
    fn default() -> Self {
        Self {
            header_read_timeout_ms: 0,
            stream_write_timeout_ms: 0,
            initial_stream_window_size: 65535,
            initial_connection_window_size: 1048576,
            max_concurrent_streams: 128,
            adaptive_window: false,
        }
    }
}
impl Http2Settings {
    fn validate(&self) -> Result<()> {
        validate_timeout(Some(self.header_read_timeout_ms))?;
        validate_timeout(Some(self.stream_write_timeout_ms))?;
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<TlsPolicy>,
    pub server_name: String,
    #[serde(default)]
    pub ca_certificate: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<ClientIdentity>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientIdentity {
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Tcp,
    Http,
    Udp,
    Http3,
}
impl Protocol {
    pub fn is_http(self) -> bool {
        matches!(self, Self::Http | Self::Http3)
    }
    pub fn is_udp(self) -> bool {
        matches!(self, Self::Udp | Self::Http3)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Listener {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http3: Option<Http3Settings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp: Option<UdpSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_idle_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls_passthrough_settings: Option<PassthroughSettings>,
    #[serde(default)]
    pub max_connections_per_ip: usize,
    #[serde(default = "connect_ms")]
    pub tls_handshake_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_protocol: Option<ProxyProtocol>,
    #[serde(default)]
    pub tls_passthrough: bool,
    #[serde(default = "yes")]
    pub access_log: bool,
    pub id: String,
    pub address: SocketAddr,
    pub protocol: Protocol,
    #[serde(default)]
    pub upstream: Option<String>,
    #[serde(default)]
    pub tls: Option<ServerTls>,
    #[serde(default = "max_conn")]
    pub max_connections: usize,
    #[serde(default = "header_ms")]
    pub header_read_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_write_timeout_ms: Option<u64>,
    /// Zero means unlimited; route values override listener values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_timeout_ms: Option<u64>,
    #[serde(default)]
    pub trusted_proxies: Vec<IpNet>,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
    #[serde(default)]
    pub routes: Vec<Route>,
    #[serde(default)]
    pub http2_settings: Http2Settings,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyProtocol {
    pub trusted_peers: Vec<IpNet>,
    pub timeout_ms: u64,
}
impl Default for ProxyProtocol {
    fn default() -> Self {
        Self {
            trusted_peers: Vec::new(),
            timeout_ms: 2000,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub static_files: Option<StaticFiles>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub methods: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<RouteHeader>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_log: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_request_body_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_body_timeout_ms: Option<u64>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "root")]
    pub path_prefix: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub upstream: String,
    #[serde(default)]
    pub plugins: Vec<Plugin>,
}
fn root() -> String {
    "/".into()
}
fn header_ms() -> u64 {
    10_000
}
fn validate_timeout(value: Option<u64>) -> Result<()> {
    ensure!(
        value.is_none_or(|v| v <= 86_400_000),
        "timeout must be 0..86400000 ms"
    );
    Ok(())
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
        ensure!(text.len() <= MAX_CONFIG_BYTES, "config exceeds 3 MiB");
        let c: Self = toml::from_str(text).context("invalid TOML")?;
        c.validate()?;
        Ok(c)
    }
    pub fn validate(&self) -> Result<()> {
        if let Some(s3) = &self.runtime.s3 {
            s3.validate()?;
        }
        if let Some(log) = &self.runtime.access_log {
            log.validate()?;
        }
        ensure!(self.schema_version == 1, "unsupported schema_version");
        ensure!(
            (1..=1_000_000).contains(&self.runtime.max_proxy_connections)
                && (1..=1_000_000).contains(&self.runtime.max_connections)
                && (1..=1_000_000).contains(&self.runtime.max_requests),
            "runtime limits must be 1..1000000"
        );
        ensure!(
            self.listeners.len() <= 128 && self.upstreams.len() <= 128,
            "too many listeners/upstreams"
        );
        ensure!(self.runtime.drain_seconds <= 3600, "invalid drain_seconds");
        ensure!(self.runtime.io_workers <= 32, "io_workers must be 0..32");
        ensure!(
            (65536..=1024 * 1024 * 1024).contains(&self.runtime.max_quic_receive_bytes),
            "max_quic_receive_bytes must be 64KiB..1GiB"
        );
        ensure!(
            (65536..=1024 * 1024 * 1024).contains(&self.runtime.max_udp_buffer_bytes),
            "max_udp_buffer_bytes must be 64KiB..1GiB"
        );
        ensure!(
            (1..=(1_u64 << 40)).contains(&self.runtime.max_static_ram_bytes),
            "runtime max_static_ram_bytes must be 1..1TiB"
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
            if let Some(retry) = &u.tcp_retry {
                ensure!(
                    (1..=8).contains(&retry.max_attempts)
                        && (1..=60000).contains(&retry.timeout_ms)
                        && retry.backoff_ms <= 1000,
                    "invalid TCP retry budgets"
                );
            }
            validate_timeout(u.half_close_timeout_ms)?;
            for tls in [u.tls.as_ref(), u.health_check.tls.as_ref()]
                .into_iter()
                .flatten()
            {
                if let Some(policy) = &tls.policy {
                    policy.validate()?;
                }
                ensure!(!tls.server_name.is_empty(), "TLS server_name is required");
                if let Some(ca) = &tls.ca_certificate {
                    TlsSource::parse(ca)?;
                }
                if let Some(id) = &tls.identity {
                    ensure!(
                        !id.certificate.as_os_str().is_empty()
                            && !id.private_key.as_os_str().is_empty(),
                        "client identity requires certificate and key"
                    );
                    TlsSource::parse(&id.certificate)?;
                    TlsSource::parse(&id.private_key)?;
                }
            }
            if let Some(policy) = &u.http {
                policy.validate()?;
            }
            if let Some(id) = u.tls.as_ref().and_then(|t| t.identity.as_ref()) {
                ensure!(
                    !id.certificate.as_os_str().is_empty()
                        && !id.private_key.as_os_str().is_empty(),
                    "client identity requires certificate and key"
                );
            }
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
                u.connect_timeout_ms > 0 && u.health_interval_ms >= 100,
                "invalid timeouts"
            );
            validate_timeout(u.response_header_timeout_ms)?;
            validate_timeout(u.response_body_timeout_ms)?;
            validate_timeout(u.write_timeout_ms)?;
            ensure!(
                u.pool_max_idle_per_host <= 4096 && u.pool_idle_timeout_ms > 0,
                "invalid upstream pool limits"
            );
            ensure!(
                (8192..=1024 * 1024).contains(&u.http1_max_buf_size),
                "HTTP/1 upstream buffer must be 8192..1048576 bytes"
            );
            u.http2_settings.validate()?;
            ensure!(
                u.http2_settings.header_read_timeout_ms == 0
                    && u.http2_settings.stream_write_timeout_ms == 0,
                "H2 header/write stream deadlines belong on listener.http2_settings"
            );
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
                h.proxy_v2_command != ProxyV2Command::Local
                    || h.send_proxy_v2.unwrap_or(u.send_proxy_v2),
                "health LOCAL command requires send_proxy_v2"
            );
            ensure!(h.port != Some(0), "health port must be positive");
            let starttls = matches!(
                h.protocol,
                HealthProtocol::SmtpStarttls
                    | HealthProtocol::ImapStarttls
                    | HealthProtocol::Pop3Starttls
            );
            ensure!(
                !starttls || h.tls.is_some() || u.tls.is_some(),
                "STARTTLS health checks require verified TLS settings"
            );
            ensure!(
                h.path.is_none()
                    || matches!(h.protocol, HealthProtocol::Connect | HealthProtocol::Http),
                "HTTP path on mail health check"
            );
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
                if host.starts_with('_') && host.contains("._tcp.") {
                    ensure!(u.dns.as_ref().is_some_and(|dns|dns.authoritative_ttl || !dns.nameservers.is_empty()),"SRV endpoint requires a wire DNS policy");
                }
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
        ensure!(
            self.listeners
                .iter()
                .flat_map(|l| &l.routes)
                .flat_map(|r| &r.headers)
                .filter(|h| h.has_regex())
                .count()
                <= 256,
            "at most 256 route header regexes per config"
        );
        for upstream in self.upstreams.values() {
            if let Some(dns) = &upstream.dns {
                dns.validate()?;
                let hosts: std::collections::BTreeSet<_> = upstream
                    .endpoints
                    .iter()
                    .filter(|e| e.address.parse::<std::net::SocketAddr>().is_err())
                    .filter_map(|e| {
                        e.address
                            .rsplit_once(':')
                            .map(|(h, _)| h.trim_end_matches('.').to_ascii_lowercase())
                    })
                    .collect();
                ensure!(
                    hosts.len() <= dns.max_hosts,
                    "DNS max_hosts is smaller than endpoint hostname count"
                );
            }
        }
        for l in &self.listeners {
            if let Some(settings) = &l.http3 {
                ensure!(
                    l.protocol == Protocol::Http3,
                    "HTTP/3 settings require HTTP/3 listener"
                );
                settings.validate()?;
            }
            if l.protocol == Protocol::Http3 {
                let settings = l.http3.clone().unwrap_or_default();
                ensure!(
                    l.tls.is_some() && !l.tls_passthrough && l.proxy_protocol.is_none(),
                    "HTTP/3 requires TLS and does not accept PROXY/passthrough"
                );
                ensure!(
                    settings.connection_receive_window as usize
                        <= self.runtime.max_quic_receive_bytes,
                    "QUIC receive budget cannot hold one connection window"
                );
            }
            if let Some(settings) = &l.udp {
                ensure!(
                    l.protocol == Protocol::Udp,
                    "UDP settings require a UDP listener"
                );
                settings.validate()?;
            }
            if l.protocol == Protocol::Udp {
                ensure!(
                    l.tls.is_none()
                        && !l.tls_passthrough
                        && l.proxy_protocol.is_none()
                        && l.plugins.is_empty()
                        && l.routes.is_empty(),
                    "UDP does not accept TLS, PROXY protocol or TCP/HTTP plugins/routes"
                );
                let upstream = l
                    .upstream
                    .as_ref()
                    .and_then(|name| self.upstreams.get(name))
                    .context("UDP requires an upstream")?;
                ensure!(upstream.tls.is_none() && !upstream.send_proxy_v2 && !upstream.health_check.enabled && upstream.http.is_none() && !upstream.http2 && upstream.tcp_retry.is_none(),"UDP upstream must be plain datagrams with health_check.enabled=false and no TCP/HTTP policy");
                let maximum = l
                    .udp
                    .as_ref()
                    .map_or(65507, |settings| settings.max_datagram_bytes);
                ensure!(
                    self.runtime.max_udp_buffer_bytes >= maximum * 2 + 128,
                    "UDP buffer budget cannot hold a maximum packet and response buffer"
                );
                ensure!(
                    upstream.balance != "consistent_hash"
                        || upstream.hash_key.as_deref().unwrap_or("$remote_addr") == "$remote_addr",
                    "UDP consistent hash requires $remote_addr"
                );
            }
            validate_timeout(l.keepalive_idle_timeout_ms)?;
            ensure!(
                l.protocol == Protocol::Http || l.keepalive_idle_timeout_ms.unwrap_or(0) == 0,
                "HTTP keepalive idle timeout requires HTTP listener"
            );
            ensure!(
                l.max_connections_per_ip <= 1_000_000,
                "per-IP connection limit exceeds 1000000"
            );
            ensure!(
                (1..=60000).contains(&l.tls_handshake_timeout_ms),
                "invalid TLS handshake timeout"
            );
            if let Some(settings) = &l.tls_passthrough_settings {
                ensure!(
                    l.tls_passthrough,
                    "passthrough settings require tls_passthrough"
                );
                settings.validate()?;
                for route in &settings.routes {
                    let upstream = self.upstreams.get(&route.upstream).with_context(|| {
                        format!("unknown passthrough route upstream {}", route.upstream)
                    })?;
                    ensure!(
                        upstream.tls.is_none(),
                        "TLS passthrough route upstream must be plain TCP"
                    );
                    ensure!(
                        upstream.balance != "consistent_hash"
                            || upstream.hash_key.as_deref().unwrap_or("$remote_addr")
                                == "$remote_addr",
                        "TCP passthrough route consistent hash requires $remote_addr"
                    );
                }
            }
            ensure!(
                !l.tls_passthrough || (l.protocol == Protocol::Tcp && l.tls.is_none()),
                "TLS passthrough requires a TCP listener without termination"
            );
            if let Some(p) = &l.proxy_protocol {
                ensure!(
                    !p.trusted_peers.is_empty()
                        && p.trusted_peers.len() <= 128
                        && (1..=60000).contains(&p.timeout_ms),
                    "PROXY protocol requires trusted_peers and bounded timeout"
                );
            }
            validate_timeout(Some(l.header_read_timeout_ms))?;
            validate_timeout(l.client_write_timeout_ms)?;
            validate_timeout(l.request_body_timeout_ms)?;
            l.http2_settings.validate()?;
            if let Some(tls) = &l.tls {
                tls.validate()?;
            }
            valid_id(&l.id)?;
            ensure!(ids.insert(&l.id), "duplicate listener id");
            ensure!(
                l.address.port() > 0 && addresses.insert((l.address, l.protocol.is_udp())),
                "duplicate or zero listener address"
            );
            ensure!(l.max_connections > 0, "listener limit must be positive");
            ensure!(
                l.max_connections <= 1_000_000 && l.routes.len() <= 4096,
                "listener limits exceeded"
            );
            if !l.protocol.is_http() {
                ensure!(
                    l.max_request_body_bytes.is_none()
                        && l.request_body_timeout_ms.is_none()
                        && l.header_read_timeout_ms == header_ms(),
                    "HTTP body/header limits on TCP listener"
                );
                ensure!(
                    !l.plugins.iter().any(|p| matches!(
                        p.name.as_str(),
                        "headers" | "rate_limit" | "rewrite" | "host_acl"
                    )),
                    "HTTP filter on TCP listener"
                );
                ensure!(
                    l.upstream.is_some() && l.routes.is_empty(),
                    "TCP requires upstream and no routes"
                );
            }
            if let Some(u) = &l.upstream {
                ensure!(self.upstreams.contains_key(u), "unknown upstream {u}");
                ensure!(
                    !l.tls_passthrough || self.upstreams[u].tls.is_none(),
                    "TLS passthrough upstream must be plain TCP"
                );
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
                r.validate_match()?;
                if let Some(files) = &r.static_files {
                    files.validate()?;
                    ensure!(
                        l.protocol.is_http() && r.upstream.is_empty(),
                        "static route requires HTTP and no upstream"
                    );
                }
                validate_timeout(r.request_body_timeout_ms)?;
                ensure!(
                    r.path_prefix.starts_with('/'),
                    "path_prefix must start with /"
                );
                ensure!(
                    r.static_files.is_some() || self.upstreams.contains_key(&r.upstream),
                    "unknown upstream {}",
                    r.upstream
                );
                validate_plugins(&r.plugins)?;
                ensure!(
                    !r.plugins.iter().any(|p| matches!(
                        p.name.as_str(),
                        "connection_limit" | "connection_rate" | "connection_window"
                    )),
                    "connection admission plugins belong on listener, not route"
                );
            }
            validate_plugins(&l.plugins)?;
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<String> {
        let text = toml::to_string_pretty(self)?;
        ensure!(
            text.len() <= MAX_CONFIG_BYTES,
            "serialized config exceeds 3 MiB; cannot safely restart"
        );
        Ok(text)
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
        if matches!(p.name.as_str(), "connection_rate" | "connection_window") {
            ensure!(
                p.hooks
                    .as_ref()
                    .is_none_or(|hooks| hooks.iter().all(|hook| hook == "connection")),
                "connection admission filters accept only the connection hook"
            );
            ensure!(p.module.is_none(), "native plugin cannot specify module");
        } else if p.name == "connection_limit" {
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
