use prost_config::DnsPolicy;
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{Mutex as AsyncMutex, Semaphore},
    time::Instant,
};
pub type LookupFuture = Pin<Box<dyn Future<Output = io::Result<Vec<IpAddr>>> + Send>>;
pub trait DnsLookup: Send + Sync {
    fn lookup(&self, host: String, max: usize) -> LookupFuture;
}
struct SystemLookup;
#[cfg(test)]
#[path = "dns_wire_tests.rs"]
mod wire_tests;
impl DnsLookup for SystemLookup {
    fn lookup(&self, host: String, max: usize) -> LookupFuture {
        Box::pin(async move {
            static CAPACITY: OnceLock<Arc<Semaphore>> = OnceLock::new();
            let permit = CAPACITY
                .get_or_init(|| Arc::new(Semaphore::new(32)))
                .clone()
                .try_acquire_owned()
                .map_err(|_| io::Error::other("DNS lookup capacity exhausted"))?;
            // Keep the permit inside the OS task after a caller timeout.
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let mut addresses = Vec::new();
                for address in (host.as_str(), 0).to_socket_addrs()? {
                    if !addresses.contains(&address.ip()) {
                        addresses.push(address.ip());
                    }
                    if addresses.len() >= max {
                        break;
                    }
                }
                if addresses.is_empty() {
                    return Err(io::Error::other("DNS returned no addresses"));
                }
                Ok(addresses)
            })
            .await
            .map_err(io::Error::other)?
        })
    }
}
struct Record {
    expires: Instant,
    ips: Vec<IpAddr>,
    error: Option<String>,
    cursor: usize,
}
pub struct DnsResolver {
    policy: DnsPolicy,
    lookup: Arc<dyn DnsLookup>,
    records: Mutex<BTreeMap<String, Arc<AsyncMutex<Record>>>>,
    revision: AtomicU64,
    wire: Option<hickory_resolver::TokioResolver>,
    services: Mutex<BTreeMap<String, Arc<AsyncMutex<ServiceRecord>>>>,
}
#[derive(Clone, PartialEq)]
struct ServiceTarget {
    priority: u16,
    weight: u16,
    addresses: Vec<SocketAddr>,
}
struct ServiceRecord {
    expires: Instant,
    targets: Vec<ServiceTarget>,
    cursor: usize,
}
pub fn dns_client(policy: &DnsPolicy) -> anyhow::Result<hickory_resolver::TokioResolver> {
    use hickory_resolver::{
        config::{ConnectionConfig, NameServerConfig, ResolverConfig},
        net::runtime::TokioRuntimeProvider,
        TokioResolver,
    };
    let mut builder = if policy.nameservers.is_empty() {
        TokioResolver::builder_tokio()?
    } else {
        let servers = policy
            .nameservers
            .iter()
            .map(|address| {
                let mut udp = ConnectionConfig::udp();
                udp.port = address.port();
                let mut tcp = ConnectionConfig::tcp();
                tcp.port = address.port();
                NameServerConfig::new(address.ip(), true, vec![udp, tcp])
            })
            .collect();
        TokioResolver::builder_with_config(
            ResolverConfig::from_name_servers(servers),
            TokioRuntimeProvider::default(),
        )
    };
    let options = builder.options_mut();
    options.cache_size = 0;
    options.attempts = 1;
    options.max_active_requests = 32;
    options.timeout = Duration::from_millis(policy.timeout_ms);
    Ok(builder.build()?)
}
impl DnsResolver {
    pub fn new(policy: DnsPolicy) -> anyhow::Result<Arc<Self>> {
        let wire = if policy.authoritative_ttl || !policy.nameservers.is_empty() {
            Some(dns_client(&policy)?)
        } else {
            None
        };
        Self::construct(policy, Arc::new(SystemLookup), wire)
    }
    pub fn with_lookup(policy: DnsPolicy, lookup: Arc<dyn DnsLookup>) -> anyhow::Result<Arc<Self>> {
        Self::construct(policy, lookup, None)
    }
    fn construct(
        policy: DnsPolicy,
        lookup: Arc<dyn DnsLookup>,
        wire: Option<hickory_resolver::TokioResolver>,
    ) -> anyhow::Result<Arc<Self>> {
        policy.validate()?;
        Ok(Arc::new(Self {
            policy,
            lookup,
            records: Mutex::new(BTreeMap::new()),
            revision: AtomicU64::new(0),
            wire,
            services: Mutex::new(BTreeMap::new()),
        }))
    }
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }
    pub async fn resolve(&self, host: &str) -> io::Result<Vec<IpAddr>> {
        let host = host
            .strip_prefix('[')
            .and_then(|h| h.strip_suffix(']'))
            .unwrap_or(host);
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Ok(vec![ip]);
        }
        if host.is_empty() || host.len() > 253 || host == "." {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "DNS hostname exceeds bounds",
            ));
        }
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        let record = {
            let mut records = self.records.lock().unwrap();
            if !records.contains_key(&host) {
                if records.len() >= self.policy.max_hosts {
                    return Err(io::Error::other("DNS host cache is full"));
                }
                records.insert(
                    host.clone(),
                    Arc::new(AsyncMutex::new(Record {
                        expires: Instant::now(),
                        ips: vec![],
                        error: None,
                        cursor: 0,
                    })),
                );
            }
            records[&host].clone()
        };
        let mut record = record.lock().await;
        if Instant::now() >= record.expires {
            let mut valid_until = None;
            let result = if let Some(wire) = &self.wire {
                match tokio::time::timeout(
                    Duration::from_millis(self.policy.timeout_ms),
                    wire.lookup_ip(host),
                )
                .await
                {
                    Ok(Ok(value)) => {
                        valid_until = Some(Instant::from_std(value.valid_until()));
                        Ok(Ok(value.iter().take(self.policy.max_addresses).collect()))
                    }
                    _ => Ok(Err(io::Error::other("DNS lookup failed"))),
                }
            } else {
                tokio::time::timeout(
                    Duration::from_millis(self.policy.timeout_ms),
                    self.lookup.lookup(host, self.policy.max_addresses),
                )
                .await
            };
            let mut ips = match result {
                Ok(Ok(ips)) => ips,
                _ => vec![],
            };
            ips.truncate(self.policy.max_addresses);
            ips.sort();
            ips.dedup();
            if ips != record.ips {
                self.revision.fetch_add(1, Ordering::AcqRel);
            }
            record.ips = ips;
            record.cursor = 0;
            record.error = if record.ips.is_empty() {
                Some("DNS lookup failed, timed out or returned no addresses".into())
            } else {
                None
            };
            record.expires = Instant::now()
                + Duration::from_millis(if record.error.is_some() {
                    self.policy.negative_ttl_ms
                } else {
                    self.policy.ttl_ms
                });
            if self.policy.authoritative_ttl && record.error.is_none() {
                if let Some(until) = valid_until {
                    record.expires = record.expires.min(until);
                }
            }
        }
        if let Some(error) = &record.error {
            return Err(io::Error::other(error.clone()));
        }
        let mut ips = record.ips.clone();
        let count = ips.len();
        ips.rotate_left(record.cursor % count);
        record.cursor = record.cursor.wrapping_add(1);
        Ok(ips)
    }
    pub async fn endpoint(&self, address: &str) -> io::Result<Vec<SocketAddr>> {
        if let Ok(address) = address.parse::<SocketAddr>() {
            return Ok(vec![address]);
        }
        let (host, port) = address.rsplit_once(':').ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "endpoint requires host:port")
        })?;
        let port = port
            .parse::<u16>()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if host.starts_with('_') && host.contains("._tcp.") {
            return self.service(host).await;
        }
        Ok(self
            .resolve(host)
            .await?
            .into_iter()
            .map(|ip| SocketAddr::new(ip, port))
            .collect())
    }
    async fn service(&self, name: &str) -> io::Result<Vec<SocketAddr>> {
        if name.len() > 253 || !name.starts_with('_') || !name.contains("._tcp.") {
            return Err(io::Error::other("invalid TCP SRV service"));
        }
        let wire = self
            .wire
            .as_ref()
            .ok_or_else(|| io::Error::other("SRV requires wire DNS policy"))?;
        let name = name.trim_end_matches('.').to_ascii_lowercase();
        let record = {
            let mut services = self.services.lock().unwrap();
            if !services.contains_key(&name) {
                if services.len() >= self.policy.max_hosts {
                    return Err(io::Error::other("SRV cache capacity"));
                }
                services.insert(
                    name.clone(),
                    Arc::new(AsyncMutex::new(ServiceRecord {
                        expires: Instant::now(),
                        targets: Vec::new(),
                        cursor: 0,
                    })),
                );
            }
            services[&name].clone()
        };
        let mut record = record.lock().await;
        if record.expires <= Instant::now() {
            let result =
                tokio::time::timeout(Duration::from_millis(self.policy.timeout_ms), async {
                    let answer = wire.srv_lookup(name).await.map_err(io::Error::other)?;
                    let mut until = Instant::from_std(answer.valid_until());
                    let mut records: Vec<_> = answer
                        .answers()
                        .iter()
                        .filter_map(|record| match &record.data {
                            hickory_resolver::proto::rr::RData::SRV(value) => Some(value.clone()),
                            _ => None,
                        })
                        .collect();
                    records.sort_by_key(|record| record.priority);
                    let mut targets = Vec::new();
                    let mut count = 0;
                    for item in records.into_iter().take(self.policy.max_addresses) {
                        if item.target.is_root() {
                            continue;
                        }
                        let Ok(ips) = wire.lookup_ip(item.target.to_utf8()).await else {
                            continue;
                        };
                        until = until.min(Instant::from_std(ips.valid_until()));
                        let addresses: Vec<_> = ips
                            .iter()
                            .take(self.policy.max_addresses - count)
                            .map(|ip| SocketAddr::new(ip, item.port))
                            .collect();
                        count += addresses.len();
                        if !addresses.is_empty() {
                            targets.push(ServiceTarget {
                                priority: item.priority,
                                weight: item.weight,
                                addresses,
                            });
                        }
                        if count >= self.policy.max_addresses {
                            break;
                        }
                    }
                    Ok::<_, io::Error>((targets, until))
                })
                .await;
            let (targets, until) = match result {
                Ok(Ok(value)) => value,
                _ => (
                    Vec::new(),
                    Instant::now() + Duration::from_millis(self.policy.negative_ttl_ms),
                ),
            };
            if targets != record.targets {
                self.revision.fetch_add(1, Ordering::AcqRel);
            }
            record.targets = targets;
            record.cursor = 0;
            record.expires = until.min(Instant::now() + Duration::from_millis(self.policy.ttl_ms));
        }
        if record.targets.is_empty() {
            return Err(io::Error::other("SRV returned no usable targets"));
        }
        let mut addresses = Vec::new();
        let cursor = record.cursor;
        record.cursor = record.cursor.wrapping_add(1);
        let mut start = 0;
        while start < record.targets.len() {
            let end = start
                + record.targets[start..]
                    .iter()
                    .take_while(|target| target.priority == record.targets[start].priority)
                    .count();
            let group = &record.targets[start..end];
            let total: usize = group.iter().map(|target| usize::from(target.weight)).sum();
            let selected = if total == 0 {
                cursor % group.len()
            } else {
                let mut ticket = cursor % total;
                group
                    .iter()
                    .position(|target| {
                        let weight = usize::from(target.weight);
                        if ticket < weight {
                            true
                        } else {
                            ticket -= weight;
                            false
                        }
                    })
                    .unwrap_or(0)
            };
            addresses.extend_from_slice(&group[selected].addresses);
            for (index, target) in group.iter().enumerate() {
                if index != selected {
                    addresses.extend_from_slice(&target.addresses);
                }
            }
            start = end;
        }
        Ok(addresses)
    }
    pub async fn connect(&self, address: &str) -> io::Result<TcpStream> {
        let addresses = self.endpoint(address).await?;
        TcpStream::connect(addresses.as_slice()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    struct Lookup {
        calls: Arc<AtomicUsize>,
        ips: Arc<Mutex<Vec<IpAddr>>>,
        wait: bool,
    }
    impl DnsLookup for Lookup {
        fn lookup(&self, _: String, _: usize) -> LookupFuture {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let ips = self.ips.lock().unwrap().clone();
            let wait = self.wait;
            Box::pin(async move {
                if wait {
                    std::future::pending::<()>().await;
                }
                tokio::task::yield_now().await;
                Ok(ips)
            })
        }
    }
    fn fixture(wait: bool) -> (Arc<DnsResolver>, Arc<AtomicUsize>, Arc<Mutex<Vec<IpAddr>>>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let ips = Arc::new(Mutex::new(vec!["127.0.0.1".parse().unwrap()]));
        let resolver = DnsResolver::with_lookup(
            DnsPolicy {
                ttl_ms: 100,
                negative_ttl_ms: 200,
                timeout_ms: 10,
                max_hosts: 1,
                max_addresses: 2,
                ..Default::default()
            },
            Arc::new(Lookup {
                calls: calls.clone(),
                ips: ips.clone(),
                wait,
            }),
        )
        .unwrap();
        (resolver, calls, ips)
    }
    #[tokio::test(start_paused = true)]
    async fn ttl_singleflight_negative_recovery_and_revision() {
        let (dns, calls, ips) = fixture(false);
        let (a, b) = tokio::join!(dns.resolve("Example.test."), dns.resolve("example.test"));
        assert_eq!(a.unwrap(), b.unwrap());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(dns.revision(), 1);
        tokio::time::advance(Duration::from_millis(101)).await;
        dns.resolve("example.test").await.unwrap();
        assert_eq!(dns.revision(), 1);
        ips.lock().unwrap().clear();
        tokio::time::advance(Duration::from_millis(101)).await;
        assert!(dns.resolve("example.test").await.is_err());
        assert_eq!(dns.revision(), 2);
        *ips.lock().unwrap() = vec!["127.0.0.2".parse().unwrap()];
        assert!(dns.resolve("example.test").await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        tokio::time::advance(Duration::from_millis(201)).await;
        assert_eq!(
            dns.resolve("example.test").await.unwrap()[0],
            "127.0.0.2".parse::<IpAddr>().unwrap()
        );
        assert_eq!(dns.revision(), 3);
    }
    #[tokio::test(start_paused = true)]
    async fn timeout_is_negative_cached_and_host_address_budgets_are_bounded() {
        let (dns, calls, _) = fixture(true);
        assert!(dns.resolve("slow.test").await.is_err());
        assert!(dns.resolve("slow.test").await.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(dns.resolve("other.test").await.is_err());
        assert_eq!(
            dns.endpoint("[::1]:443").await.unwrap(),
            vec!["[::1]:443".parse::<SocketAddr>().unwrap()]
        );
        assert!(dns.resolve("").await.is_err());
        assert!(DnsResolver::new(DnsPolicy {
            max_addresses: 0,
            ..Default::default()
        })
        .is_err());
    }
    #[tokio::test]
    async fn multiple_addresses_rotate_without_changing_revision() {
        let (dns, _, ips) = fixture(false);
        *ips.lock().unwrap() = vec![
            "127.0.0.2".parse().unwrap(),
            "127.0.0.1".parse().unwrap(),
            "127.0.0.3".parse().unwrap(),
        ];
        let a = dns.endpoint("example.test:25").await.unwrap();
        let b = dns.endpoint("example.test:25").await.unwrap();
        assert_eq!(a.len(), 2);
        assert_eq!(a[0], b[1]);
        assert_eq!(a[1], b[0]);
        assert_eq!(dns.revision(), 1);
    }
}
