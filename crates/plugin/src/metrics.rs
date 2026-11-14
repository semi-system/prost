use crate::{
    Action, Event, FilterCall, FilterDecision, FilterHook, HeaderContext, NativeFilter,
    NativePlugin,
};
use anyhow::Result;
use http::{HeaderMap, HeaderName, Uri};
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

pub const HOOKS: [&str; 6] = [
    "connection",
    "upstream",
    "request_headers",
    "response_headers",
    "end",
    "trailers",
];
#[derive(Default)]
pub struct PluginMetrics {
    results: [[AtomicU64; 4]; 6],
    nanos: [AtomicU64; 6],
}
pub struct PluginSnapshot {
    pub results: [[u64; 4]; 6],
    pub nanos: [u64; 6],
}
impl PluginMetrics {
    pub fn snapshot(&self) -> PluginSnapshot {
        PluginSnapshot {
            results: std::array::from_fn(|h| {
                std::array::from_fn(|r| self.results[h][r].load(Ordering::Relaxed))
            }),
            nanos: std::array::from_fn(|h| self.nanos[h].load(Ordering::Relaxed)),
        }
    }
    pub fn start(&self, hook: &str) -> Call<'_> {
        Call {
            stats: self,
            index: HOOKS.iter().position(|h| *h == hook).unwrap_or(0),
            started: Instant::now(),
            result: 3,
        }
    }
}
pub struct Call<'a> {
    stats: &'a PluginMetrics,
    index: usize,
    started: Instant,
    result: usize,
}
impl Call<'_> {
    pub fn finish<T>(&mut self, result: &Result<T>, rejected: bool) {
        self.result = if result.is_err() {
            1
        } else if rejected {
            2
        } else {
            0
        };
    }
}
impl Drop for Call<'_> {
    fn drop(&mut self) {
        self.stats.results[self.index][self.result].fetch_add(1, Ordering::Relaxed);
        self.stats.nanos[self.index].fetch_add(
            self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}
fn hook(h: FilterHook) -> &'static str {
    match h {
        FilterHook::Connection => "connection",
        FilterHook::RequestHeaders => "request_headers",
        FilterHook::ResponseHeaders => "response_headers",
    }
}
fn rejected(result: &Result<FilterDecision>) -> bool {
    matches!(result, Ok(FilterDecision::Reject { .. }))
}
pub struct MeasuredFilter {
    pub inner: Arc<dyn NativeFilter>,
    pub metrics: Arc<PluginMetrics>,
}
impl NativeFilter for MeasuredFilter {
    fn rate_table(&self) -> Option<&dyn crate::rate_table::RateTable> {
        self.inner.rate_table()
    }
    fn applies_to(&self, h: FilterHook) -> bool {
        self.inner.applies_to(h)
    }
    fn uses_request_uri(&self) -> bool {
        self.inner.uses_request_uri()
    }
    fn header_inputs(&self, u: bool) -> &[HeaderName] {
        self.inner.header_inputs(u)
    }
    fn context_needs(&self) -> u8 {
        self.inner.context_needs()
    }
    fn parent_override(&self) -> Option<(u8, bool)> {
        self.inner.parent_override()
    }
    fn filter(
        &self,
        h: FilterHook,
        p: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        let mut call = self.metrics.start(hook(h));
        let result = self.inner.filter(h, p, headers);
        call.finish(&result, rejected(&result));
        result
    }
    fn filter_with_context(
        &self,
        c: &FilterCall<'_>,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        let mut call = self.metrics.start(hook(c.hook));
        let result = self.inner.filter_with_context(c, headers);
        call.finish(&result, rejected(&result));
        result
    }
    fn filter_request(
        &self,
        c: &FilterCall<'_>,
        uri: &mut Uri,
        headers: &mut HeaderMap,
    ) -> Result<FilterDecision> {
        let mut call = self.metrics.start(hook(c.hook));
        let result = self.inner.filter_request(c, uri, headers);
        call.finish(&result, rejected(&result));
        result
    }
    fn trailers(
        &self,
        s: u16,
        c: Option<&HeaderContext>,
        headers: &HeaderMap,
    ) -> Result<Option<HeaderMap>> {
        let mut call = self.metrics.start("trailers");
        let result = self.inner.trailers(s, c, headers);
        call.finish(&result, false);
        result
    }
}
pub struct MeasuredNative {
    pub inner: Arc<dyn NativePlugin>,
    pub metrics: Arc<PluginMetrics>,
}
impl NativePlugin for MeasuredNative {
    fn on_event(&self, event: &Event) -> Result<Action> {
        let mut call = self.metrics.start(&event.hook);
        let result = self.inner.on_event(event);
        call.finish(&result, result.as_ref().is_ok_and(|a| a.deny));
        result
    }
}
