//! Small, synchronous native filters: no event serialization or blocking tasks.
use anyhow::{ensure, Result};
use http::HeaderMap;
use ipnet::IpNet;
use serde::Deserialize;
use std::net::IpAddr;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FilterHook {
    Connection,
    RequestHeaders,
    ResponseHeaders,
}
impl FilterHook {
    pub(crate) fn from_str(hook: &str) -> Option<Self> {
        match hook {
            "connection" => Some(Self::Connection),
            "request_headers" => Some(Self::RequestHeaders),
            "response_headers" => Some(Self::ResponseHeaders),
            _ => None,
        }
    }
}
pub enum FilterDecision {
    Continue,
    Redirect(std::sync::Arc<Redirect>),
    Reject {
        status: u16,
        retry_after: Option<u64>,
    },
}
#[derive(Debug)]
pub struct Redirect {
    pub status: u16,
    pub location: http::HeaderValue,
}
pub trait NativeFilter: Send + Sync {
    fn rate_table(&self) -> Option<&dyn crate::rate_table::RateTable> {
        None
    }
    fn uses_request_uri(&self) -> bool {
        false
    }
    fn filter_request(
        &self,
        call: &FilterCall<'_>,
        _: &mut http::Uri,
        headers: &mut HeaderMap,
    ) -> Result<FilterDecision> {
        self.filter_with_context(call, Some(headers))
    }
    fn header_inputs(&self, _: bool) -> &[http::HeaderName] {
        &[]
    }
    fn context_needs(&self) -> u8 {
        0
    }
    fn parent_override(&self) -> Option<(u8, bool)> {
        None
    }
    fn filter_with_context(
        &self,
        call: &FilterCall<'_>,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        self.filter(call.hook, call.peer, headers)
    }
    fn trailers(
        &self,
        _: u16,
        _: Option<&crate::HeaderContext>,
        _: &HeaderMap,
    ) -> Result<Option<HeaderMap>> {
        Ok(None)
    }
    fn applies_to(&self, hook: FilterHook) -> bool;
    /// Must return promptly without blocking. Called on an I/O worker.
    fn filter(
        &self,
        hook: FilterHook,
        peer: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision>;
}
#[derive(Clone, Copy)]
pub struct FilterCall<'a> {
    pub hook: FilterHook,
    pub peer: IpAddr,
    pub status: u16,
    pub context: Option<&'a crate::HeaderContext>,
    pub inherited_skip: u8,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AccessConfig {
    require_allowlist: bool,
    allow: Vec<IpNet>,
    deny: Vec<IpNet>,
}
pub(crate) struct IpAccess {
    config: AccessConfig,
}
impl IpAccess {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: AccessConfig = serde_json::from_value(value)?;
        ensure!(
            !config.require_allowlist
                || (!config.allow.is_empty() && config.allow.iter().all(|n| n.prefix_len() > 0)),
            "required allow-list must be nonempty and must not contain an IPv4/IPv6 /0"
        );
        ensure!(
            config.allow.len() + config.deny.len() <= 256,
            "too many ACL rules"
        );
        Ok(Self { config })
    }
}
#[cfg(test)]
mod allowlist_tests {
    use super::*;
    #[test]
    fn required_allowlist_rejects_empty_and_both_default_routes() {
        for allow in [
            serde_json::json!([]),
            serde_json::json!(["0.0.0.0/0"]),
            serde_json::json!(["::/0"]),
        ] {
            assert!(
                IpAccess::new(serde_json::json!({"require_allowlist":true,"allow":allow})).is_err()
            );
        }
        assert!(IpAccess::new(
            serde_json::json!({"require_allowlist":true,"allow":["192.0.2.0/24","2001:db8::/32"]})
        )
        .is_ok());
    }
}
impl NativeFilter for IpAccess {
    fn applies_to(&self, hook: FilterHook) -> bool {
        matches!(hook, FilterHook::Connection | FilterHook::RequestHeaders)
    }
    fn filter(
        &self,
        _: FilterHook,
        peer: IpAddr,
        _: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        let allowed = !self.config.deny.iter().any(|n| n.contains(&peer))
            && (self.config.allow.is_empty()
                || self.config.allow.iter().any(|n| n.contains(&peer)));
        Ok(if allowed {
            FilterDecision::Continue
        } else {
            FilterDecision::Reject {
                status: 403,
                retry_after: None,
            }
        })
    }
}

mod host;
mod rate;
mod window;
pub(crate) use host::HostAccess;
pub(crate) use rate::RateLimit;
pub(crate) use window::ConnectionWindow;
