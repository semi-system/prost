use super::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use anyhow::{ensure, Result};
use http::{header, uri::Authority, HeaderMap, Uri};
use serde::Deserialize;
use std::{borrow::Cow, collections::HashSet, net::IpAddr};

fn status() -> u16 {
    421
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    hosts: Vec<String>,
    #[serde(default = "status")]
    status: u16,
}
pub(crate) struct HostAccess {
    hosts: HashSet<String>,
    status: u16,
}
fn hostname(authority: &Authority) -> Option<Cow<'_, str>> {
    if authority.as_str().contains('@') {
        return None;
    }
    let host = authority.host();
    if host.is_empty() {
        return None;
    }
    // http::Authority may retain a nonnumeric suffix while port() returns None.
    let raw = authority.as_str();
    let suffix = if raw.starts_with('[') {
        &raw[raw.find(']')? + 1..]
    } else {
        if raw.bytes().filter(|b| *b == b':').count() > 1 {
            return None;
        }
        raw.find(':').map_or("", |i| &raw[i..])
    };
    if !suffix.is_empty() {
        let port = suffix.strip_prefix(':')?;
        if port.is_empty()
            || !port.bytes().all(|b| b.is_ascii_digit())
            || port.parse::<u16>().is_err()
        {
            return None;
        }
    }
    let host = host
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host);
    let host = host.strip_suffix('.').unwrap_or(host);
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Some(Cow::Owned(ip.to_string()));
    }
    Some(if host.bytes().any(|b| b.is_ascii_uppercase()) {
        Cow::Owned(host.to_ascii_lowercase())
    } else {
        Cow::Borrowed(host)
    })
}
impl HostAccess {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: Config = serde_json::from_value(value)?;
        ensure!(
            !config.hosts.is_empty()
                && config.hosts.len() <= 256
                && (400..=599).contains(&config.status),
            "invalid host_acl host count/status"
        );
        let mut hosts = HashSet::new();
        for host in config.hosts {
            let authority: Authority = host.parse()?;
            ensure!(
                authority.port().is_none() && host.len() <= 253,
                "host_acl expects hosts without ports"
            );
            let name = hostname(&authority).ok_or_else(|| anyhow::anyhow!("invalid ACL host"))?;
            ensure!(!name.contains('*'), "host_acl uses exact hosts");
            ensure!(
                name.parse::<IpAddr>().is_ok()
                    || prost_config::ServerTls::normalize_name(&name).is_ok(),
                "invalid ACL DNS host"
            );
            ensure!(hosts.insert(name.into_owned()), "duplicate ACL host");
        }
        Ok(Self {
            hosts,
            status: config.status,
        })
    }
    fn decide(&self, uri: Option<&Uri>, headers: &HeaderMap) -> FilterDecision {
        let allowed = (|| {
            if headers.get_all(header::HOST).iter().count() > 1 {
                return None;
            }
            let explicit = match headers.get(header::HOST) {
                Some(value) => Some(value.to_str().ok()?.parse::<Authority>().ok()?),
                None => None,
            };
            let authority = uri.and_then(Uri::authority).or(explicit.as_ref())?;
            let name = hostname(authority)?;
            if let (Some(uri_authority), Some(host)) =
                (uri.and_then(Uri::authority), explicit.as_ref())
            {
                let default = uri.and_then(Uri::scheme_str).and_then(|s| match s {
                    "https" => Some(443),
                    "http" => Some(80),
                    _ => None,
                });
                if hostname(uri_authority)? != hostname(host)?
                    || uri_authority.port_u16().or(default) != host.port_u16().or(default)
                {
                    return None;
                }
            }
            self.hosts.contains(name.as_ref()).then_some(())
        })()
        .is_some();
        if allowed {
            FilterDecision::Continue
        } else {
            FilterDecision::Reject {
                status: self.status,
                retry_after: None,
            }
        }
    }
}
impl NativeFilter for HostAccess {
    fn uses_request_uri(&self) -> bool {
        true
    }
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == FilterHook::RequestHeaders
    }
    fn filter_request(
        &self,
        _: &FilterCall<'_>,
        uri: &mut Uri,
        headers: &mut HeaderMap,
    ) -> Result<FilterDecision> {
        Ok(self.decide(Some(uri), headers))
    }
    fn filter(
        &self,
        _: FilterHook,
        _: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        Ok(headers.map_or(
            FilterDecision::Reject {
                status: self.status,
                retry_after: None,
            },
            |headers| self.decide(None, headers),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn authority_host_conflicts_missing_duplicates_and_exact_hosts() -> Result<()> {
        let filter = HostAccess::new(serde_json::json!({"hosts":["public.test","[::1]"]}))?;
        for (uri, host, allow) in [
            ("https://public.test/", Some("PUBLIC.TEST:443"), true),
            ("https://public.test/", Some("evil.test"), false),
            ("https://public.test:443/", Some("public.test:80"), false),
            ("/", None, false),
            ("/", Some("public.test:bad"), false),
            ("/", Some("user@public.test"), false),
            ("/", Some("[0:0:0:0:0:0:0:1]:8080"), true),
        ] {
            let uri: Uri = uri.parse()?;
            let mut headers = HeaderMap::new();
            if let Some(host) = host {
                headers.insert(header::HOST, host.parse()?);
            }
            assert_eq!(
                matches!(
                    filter.decide(Some(&uri), &headers),
                    FilterDecision::Continue
                ),
                allow,
                "{uri} {host:?}"
            );
        }
        let mut headers = HeaderMap::new();
        headers.append(header::HOST, "public.test".parse()?);
        headers.append(header::HOST, "evil.test".parse()?);
        assert!(matches!(
            filter.decide(None, &headers),
            FilterDecision::Reject { status: 421, .. }
        ));
        assert!(HostAccess::new(serde_json::json!({"hosts":["*.test"]})).is_err());
        Ok(())
    }
}
