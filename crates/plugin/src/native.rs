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
    Reject {
        status: u16,
        retry_after: Option<u64>,
    },
}
pub trait NativeFilter: Send + Sync {
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
            config.allow.len() + config.deny.len() <= 256,
            "too many ACL rules"
        );
        Ok(Self { config })
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

mod rate;
pub(crate) use rate::RateLimit;
