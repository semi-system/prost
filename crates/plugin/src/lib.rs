use anyhow::{bail, ensure, Context, Result};
mod headers;
mod native;
pub use headers::{
    HeaderContext, REQUEST_HEADERS as NEEDS_REQUEST_HEADERS, REQUEST_ID as NEEDS_REQUEST_ID,
    UPSTREAM_HEADERS as NEEDS_UPSTREAM_HEADERS,
};
pub use native::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use prost_config::Plugin;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use wasmtime::{Engine, Instance, Module, Store, StoreLimits, StoreLimitsBuilder};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    pub abi_version: u32,
    pub hook: String,
    pub peer: String,
    pub upstream: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub config: serde_json::Value,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    #[serde(default)]
    pub deny: bool,
    #[serde(default)]
    pub upstream: Option<String>,
    #[serde(default)]
    pub set_headers: BTreeMap<String, String>,
}
pub trait NativePlugin: Send + Sync {
    fn on_event(&self, event: &Event) -> Result<Action>;
}
pub struct PluginRegistry {
    engine: once_cell::sync::OnceCell<Engine>,
    capacity: Arc<Semaphore>,
    native: BTreeMap<String, Arc<dyn NativePlugin>>,
    filters: BTreeMap<String, Arc<dyn NativeFilter>>,
}
impl PluginRegistry {
    pub fn new() -> Result<Self> {
        Ok(Self {
            engine: once_cell::sync::OnceCell::new(),
            capacity: Arc::new(Semaphore::new(32)),
            native: BTreeMap::new(),
            filters: BTreeMap::new(),
        })
    }
    pub fn engine(&self) -> Result<&Engine> {
        self.engine.get_or_try_init(|| {
            let mut config = wasmtime::Config::new();
            config.consume_fuel(true);
            config.max_wasm_stack(256 * 1024);
            Engine::new(&config)
        })
    }
    pub fn register(&mut self, name: String, plugin: Arc<dyn NativePlugin>) {
        self.native.insert(name, plugin);
    }
    pub fn register_filter(&mut self, name: String, filter: Arc<dyn NativeFilter>) {
        self.filters.insert(name, filter);
    }
    pub fn prepare(&self, p: &Plugin) -> Result<PreparedPlugin> {
        let plugin = self.prepare_inner(p)?;
        Ok(if let Some(hooks) = &p.hooks {
            PreparedPlugin::Filtered(
                Arc::new(plugin),
                hooks.iter().fold(0, |mask, hook| mask | hook_bit(hook)),
            )
        } else {
            plugin
        })
    }
    fn prepare_inner(&self, p: &Plugin) -> Result<PreparedPlugin> {
        let value = || serde_json::to_value(&p.config);
        match p.name.as_str() {
            "headers" => {
                return Ok(PreparedPlugin::Fast(Arc::new(headers::Headers::new(
                    value()?,
                )?)))
            }
            "ip_acl" => {
                return Ok(PreparedPlugin::Fast(Arc::new(native::IpAccess::new(
                    value()?,
                )?)))
            }
            "rate_limit" => {
                return Ok(PreparedPlugin::Fast(Arc::new(native::RateLimit::new(
                    value()?,
                )?)))
            }
            _ => {}
        }
        if p.name == "connection_limit" {
            let n = p.config["max_connections"]
                .as_integer()
                .context("max_connections")? as usize;
            return Ok(PreparedPlugin::Limit(Arc::new(Semaphore::new(n))));
        }
        if let Some(native) = self.native.get(&p.name) {
            return Ok(PreparedPlugin::Native(native.clone()));
        }
        if let Some(filter) = self.filters.get(&p.name) {
            return Ok(PreparedPlugin::Fast(filter.clone()));
        }
        ensure!(p.name == "wasm", "unknown plugin {}", p.name);
        let engine = self.engine()?;
        let module = Module::from_file(engine, p.module.as_ref().context("module required")?)?;
        let config = serde_json::to_value(&p.config)?;
        let prepared = Wasm {
            engine: engine.clone(),
            module,
            config,
            capacity: self.capacity.clone(),
        };
        // Instantiate and verify ABI without allowing any host imports or WASI.
        prepared.check()?;
        Ok(PreparedPlugin::Wasm(Arc::new(prepared)))
    }
}
pub enum PreparedPlugin {
    Fast(Arc<dyn NativeFilter>),
    Filtered(Arc<PreparedPlugin>, u8),
    Limit(Arc<Semaphore>),
    Native(Arc<dyn NativePlugin>),
    Wasm(Arc<Wasm>),
}
fn hook_bit(hook: &str) -> u8 {
    match hook {
        "connection" => 1,
        "upstream" => 2,
        "request_headers" => 4,
        "response_headers" => 8,
        "end" => 16,
        _ => 0,
    }
}
impl PreparedPlugin {
    pub fn copy_header_inputs(
        &self,
        upstream: bool,
        source: &http::HeaderMap,
        target: &mut http::HeaderMap,
    ) {
        match self {
            Self::Filtered(inner, _) => {
                if self.has_hooks() {
                    inner.copy_header_inputs(upstream, source, target)
                }
            }
            Self::Fast(p) => {
                for name in p.header_inputs(upstream) {
                    if !target.contains_key(name) {
                        for value in source.get_all(name).iter() {
                            target.append(name.clone(), value.clone());
                        }
                    }
                }
            }
            _ => {}
        }
    }
    pub fn context_needs(&self) -> u8 {
        match self {
            Self::Filtered(inner, _) => inner.context_needs(),
            Self::Fast(p) => p.context_needs(),
            _ => 0,
        }
    }
    pub fn parent_override(&self) -> Option<(u8, bool)> {
        match self {
            Self::Filtered(inner, _) => inner.parent_override(),
            Self::Fast(p) => p.parent_override(),
            _ => None,
        }
    }
    pub fn filter_call(
        &self,
        call: &FilterCall<'_>,
        headers: Option<&mut http::HeaderMap>,
    ) -> Result<FilterDecision> {
        match self {
            Self::Filtered(inner, _) => inner.filter_call(call, headers),
            Self::Fast(p) => p.filter_with_context(call, headers),
            _ => Ok(FilterDecision::Continue),
        }
    }
    pub fn trailers(
        &self,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &http::HeaderMap,
    ) -> Result<Option<http::HeaderMap>> {
        if !self.has_hook("response_headers") {
            return Ok(None);
        }
        match self {
            Self::Filtered(inner, _) => inner.trailers(status, ctx, headers),
            Self::Fast(p) => p.trailers(status, ctx, headers),
            _ => Ok(None),
        }
    }
    pub fn has_event_hooks(&self) -> bool {
        match self {
            Self::Filtered(inner, _) => self.has_hooks() && inner.has_event_hooks(),
            Self::Fast(_) | Self::Limit(_) => false,
            _ => true,
        }
    }
    pub fn is_filter(&self) -> bool {
        match self {
            Self::Filtered(inner, _) => inner.is_filter(),
            Self::Fast(_) => true,
            _ => false,
        }
    }
    pub fn filter(
        &self,
        hook: &str,
        peer: std::net::IpAddr,
        headers: Option<&mut http::HeaderMap>,
    ) -> Result<FilterDecision> {
        let mut plugin = self;
        while let Self::Filtered(inner, _) = plugin {
            plugin = inner;
        }
        if let (Self::Fast(filter), Some(hook)) = (plugin, FilterHook::from_str(hook)) {
            filter.filter(hook, peer, headers)
        } else {
            Ok(FilterDecision::Continue)
        }
    }
    pub fn has_hooks(&self) -> bool {
        [
            "connection",
            "upstream",
            "request_headers",
            "response_headers",
            "end",
        ]
        .iter()
        .any(|hook| self.has_hook(hook))
    }
    pub fn has_hook(&self, hook: &str) -> bool {
        let mut plugin = self;
        while let Self::Filtered(inner, hooks) = plugin {
            if hooks & hook_bit(hook) == 0 {
                return false;
            }
            plugin = inner;
        }
        match plugin {
            Self::Fast(filter) => FilterHook::from_str(hook).is_some_and(|h| filter.applies_to(h)),
            Self::Limit(_) => false,
            _ => true,
        }
    }
    pub fn acquire_connection(&self) -> Result<Option<OwnedSemaphorePermit>> {
        let mut plugin = self;
        while let Self::Filtered(inner, _) = plugin {
            plugin = inner;
        }
        match plugin {
            Self::Limit(s) => Ok(Some(
                s.clone()
                    .try_acquire_owned()
                    .context("plugin connection limit")?,
            )),
            _ => Ok(None),
        }
    }
    pub async fn invoke(&self, event: &Event) -> Result<Action> {
        let mut plugin = self;
        while let Self::Filtered(inner, hooks) = plugin {
            if hooks & hook_bit(&event.hook) == 0 {
                return Ok(Action::default());
            }
            plugin = inner;
        }
        match plugin {
            Self::Filtered(_, _) => unreachable!("filters unwrapped"),
            Self::Limit(_) | Self::Fast(_) => Ok(Action::default()),
            Self::Native(p) => p.on_event(event),
            Self::Wasm(w) => {
                let permit = w
                    .capacity
                    .clone()
                    .try_acquire_owned()
                    .context("WASM capacity exhausted")?;
                let w = w.clone();
                let mut event = event.clone();
                event.config = w.config.clone();
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    w.call(&event)
                })
                .await?
            }
        }
    }
}
pub struct Wasm {
    engine: Engine,
    module: Module,
    config: serde_json::Value,
    capacity: Arc<Semaphore>,
}
impl Wasm {
    fn instantiate(&self) -> Result<(Store<StoreLimits>, Instance)> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(8 * 1024 * 1024)
            .table_elements(1024)
            .instances(1)
            .memories(1)
            .tables(1)
            .build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|s| s);
        store.set_fuel(100_000)?;
        let instance = Instance::new(&mut store, &self.module, &[])?;
        Ok((store, instance))
    }
    fn check(&self) -> Result<()> {
        let (mut s, i) = self.instantiate()?;
        ensure!(
            i.get_memory(&mut s, "memory").is_some(),
            "WASM memory export missing"
        );
        let version = i
            .get_typed_func::<(), i32>(&mut s, "prost_abi_version")?
            .call(&mut s, ())?;
        ensure!(version == 1, "unsupported WASM ABI");
        i.get_typed_func::<i32, i32>(&mut s, "alloc")?;
        i.get_typed_func::<(i32, i32), i64>(&mut s, "handle")?;
        Ok(())
    }
    fn call(&self, event: &Event) -> Result<Action> {
        let input = serde_json::to_vec(event)?;
        ensure!(input.len() <= 64 * 1024, "plugin input too large");
        let (mut s, i) = self.instantiate()?;
        let memory = i.get_memory(&mut s, "memory").context("memory")?;
        let alloc = i.get_typed_func::<i32, i32>(&mut s, "alloc")?;
        let handle = i.get_typed_func::<(i32, i32), i64>(&mut s, "handle")?;
        let ptr = alloc.call(&mut s, input.len() as i32)?;
        ensure!(ptr >= 0, "invalid input pointer");
        memory.write(&mut s, ptr as usize, &input)?;
        let packed = handle.call(&mut s, (ptr, input.len() as i32))? as u64;
        let out = (packed >> 32) as usize;
        let len = (packed & 0xffff_ffff) as usize;
        ensure!(len <= 64 * 1024, "plugin output too large");
        let mut result = vec![0u8; len];
        memory.read(&s, out, &mut result)?;
        let action: Action = serde_json::from_slice(&result)?;
        if action.set_headers.len() > 64 {
            bail!("too many plugin headers");
        }
        Ok(action)
    }
}
