#![cfg(unix)]
use anyhow::{ensure, Context, Result};
pub use prost_core::{CacheKeyInput, CacheQuery};
use prost_core::{GcOptions, RunningProxy};
pub use prost_core::{KeyEncoding, RateTableTarget, TableKey, TableQuery};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    os::unix::fs::{FileTypeExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    sync::Semaphore,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub const MAX_FRAME: usize = 4 * 1024 * 1024;
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub version: u32,
    pub id: String,
    #[serde(flatten)]
    pub command: Command,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum Command {
    Validate {
        config: String,
    },
    Apply {
        config: String,
        expected_generation: u64,
    },
    #[serde(rename = "tls.reload")]
    ReloadTls {
        listener: String,
        tls: Box<prost_core::prost_config::ServerTls>,
        expected_generation: u64,
    },
    #[serde(rename = "tls.reload_many")]
    ReloadTlsMany {
        updates: std::collections::BTreeMap<String, prost_core::prost_config::ServerTls>,
        expected_generation: u64,
    },
    #[serde(rename = "upstream.set_enabled")]
    SetEnabled {
        endpoint: String,
        enabled: bool,
        expected_generation: u64,
    },
    Status,
    #[serde(rename = "reload.wait")]
    WaitWorkers {
        generation: u64,
        timeout_ms: u64,
    },
    #[serde(rename = "config.export")]
    Export,
    #[serde(rename = "cache.status")]
    CacheStatus {
        #[serde(default)]
        upstream: Option<String>,
    },
    #[serde(rename = "cache.entries")]
    CacheEntries {
        upstream: String,
        #[serde(default)]
        query: CacheQuery,
    },
    #[serde(rename = "cache.key")]
    CacheKey {
        input: CacheKeyInput,
    },
    #[serde(rename = "cache.purge")]
    CachePurge {
        upstream: String,
        #[serde(default)]
        key: Option<String>,
        #[serde(default)]
        all: bool,
        expected_generation: u64,
    },
    #[serde(rename = "rate_table.list")]
    RateTableList {
        #[serde(default)]
        listener: Option<String>,
        #[serde(default = "rate_table_limit")]
        limit: usize,
    },
    #[serde(rename = "rate_table.show")]
    RateTableShow {
        target: RateTableTarget,
        #[serde(default)]
        query: TableQuery,
    },
    #[serde(rename = "rate_table.clear")]
    RateTableClear {
        target: RateTableTarget,
        #[serde(default)]
        key: Option<TableKey>,
        #[serde(default)]
        all: bool,
        expected_generation: u64,
    },
    #[serde(rename = "artifacts.gc")]
    CollectArtifacts {
        #[serde(default = "gc_dry_run")]
        dry_run: bool,
        #[serde(default = "gc_min_age")]
        min_age_seconds: u64,
        #[serde(default = "gc_max_entries")]
        max_entries: usize,
        #[serde(default = "gc_max_delete")]
        max_delete_files: u64,
        expected_generation: u64,
    },
    #[serde(rename = "access_log.flush")]
    FlushAccessLog {
        expected_generation: u64,
    },
    #[serde(rename = "access_log.reopen")]
    ReopenAccessLog {
        expected_generation: u64,
    },
}
fn rate_table_limit() -> usize {
    100
}
fn gc_dry_run() -> bool {
    true
}
fn gc_min_age() -> u64 {
    GcOptions::default().min_age_seconds
}
fn gc_max_entries() -> usize {
    GcOptions::default().max_entries
}
fn gc_max_delete() -> u64 {
    GcOptions::default().max_delete_files
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub version: u32,
    pub id: String,
    pub generation: u64,
    pub result: Option<Value>,
    pub error: Option<ApiError>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
}
pub async fn read_frame<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    io: &mut R,
) -> Result<T> {
    let len = io.read_u32().await? as usize;
    ensure!(len > 0 && len <= MAX_FRAME, "frame exceeds limit");
    let mut bytes = vec![0; len];
    io.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
pub async fn write_frame<W: AsyncWrite + Unpin, T: Serialize>(io: &mut W, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "response exceeds frame limit");
    io.write_u32(bytes.len() as u32).await?;
    io.write_all(&bytes).await?;
    io.flush().await?;
    Ok(())
}
pub async fn request(path: &Path, command: Command) -> Result<Response> {
    let mut io = UnixStream::connect(path)
        .await
        .with_context(|| format!("connect {}", path.display()))?;
    let request = Request {
        version: 1,
        id: format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ),
        command,
    };
    write_frame(&mut io, &request).await?;
    let response: Response = read_frame(&mut io).await?;
    ensure!(
        response.id == request.id && response.version == 1,
        "invalid response envelope"
    );
    if let Some(e) = &response.error {
        anyhow::bail!(
            "{}: {} (generation {})",
            e.code,
            e.message,
            response.generation
        );
    }
    Ok(response)
}
pub struct ControlServer {
    cancel: CancellationToken,
    tasks: TaskTracker,
    path: PathBuf,
}
impl ControlServer {
    pub async fn bind(proxy: Arc<RunningProxy>) -> Result<Self> {
        let path = proxy.snapshot().config.runtime.admin_socket.clone();
        let parent = path.parent().context("socket parent")?;
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&path) {
            ensure!(
                metadata.file_type().is_socket(),
                "admin path exists and is not socket"
            );
            match UnixStream::connect(&path).await {
                Ok(_) => anyhow::bail!("admin socket already active"),
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::fs::remove_file(&path)?
                }
                Err(e) => return Err(e.into()),
            }
        }
        // Bind under a private temporary directory, set mode before exposing socket.
        let staging = parent.join(format!(".prost-socket-{}", std::process::id()));
        std::fs::create_dir(&staging)?;
        std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o700))?;
        let temporary = staging.join("admin.sock");
        let listener = UnixListener::bind(&temporary)?;
        std::fs::set_permissions(&temporary, std::fs::Permissions::from_mode(0o600))?;
        std::fs::rename(&temporary, &path)?;
        std::fs::remove_dir(&staging)?;
        let cancel = CancellationToken::new();
        let tasks = TaskTracker::new();
        let token = cancel.clone();
        let tracker = tasks.clone();
        let capacity = Arc::new(Semaphore::new(32));
        let wait_capacity = Arc::new(Semaphore::new(8));
        tasks.spawn(async move {
            loop {
                let accepted=tokio::select!{_=token.cancelled()=>break,a=listener.accept()=>a};
                let Ok((mut io,_))=accepted else {continue};
                let Ok(permit)=capacity.clone().try_acquire_owned() else {continue};
                let p=proxy.clone();let token=token.clone();let wait_capacity=wait_capacity.clone();
                tracker.spawn(async move {
                    let _permit=permit;
                    let work=async {
                        let req:Request=tokio::time::timeout(Duration::from_secs(10),read_frame(&mut io)).await??;
                        let response=if matches!(&req.command,Command::WaitWorkers {..}) {
                            tokio::select! {
                                response=dispatch(p,req,wait_capacity)=>response,
                                _=io.read_u8()=>anyhow::bail!("worker wait client disconnected or sent unexpected data"),
                            }
                        } else {dispatch(p,req,wait_capacity).await};
                        tokio::time::timeout(Duration::from_secs(10),write_frame(&mut io,&response)).await??;
                        Ok::<_,anyhow::Error>(())
                    };
                    tokio::select!{_=token.cancelled()=>{},r=work=>{if let Err(e)=r { tracing::debug!(error=%e,"control session ended"); }}}
                });
            }
        });
        Ok(Self {
            cancel,
            tasks,
            path,
        })
    }
    pub async fn shutdown(self) {
        self.cancel.cancel();
        self.tasks.close();
        self.tasks.wait().await;
        let _ = std::fs::remove_file(self.path);
    }
}
async fn dispatch(
    proxy: Arc<RunningProxy>,
    request: Request,
    wait_capacity: Arc<Semaphore>,
) -> Response {
    let result = async {
        ensure!(request.version == 1, "unsupported_api_version");
        match request.command {
            Command::Status => Ok(proxy.status()),
            Command::WaitWorkers {generation,timeout_ms} => {
                let _permit=wait_capacity.try_acquire().context("too_many_worker_waiters")?;
                proxy.wait_for_workers(generation,timeout_ms).await
            },
            Command::Export => Ok(json!({"config":proxy.snapshot().config.encode()?})),
            Command::CacheStatus {upstream}=>proxy.cache_status(upstream).await,
            Command::CacheEntries {upstream,query}=>proxy.cache_entries(upstream,query).await,
            Command::CacheKey {input}=>Ok(json!({"key":prost_core::cache_key(&input)?,"generation":proxy.snapshot().config.generation})),
            Command::CachePurge {upstream,key,all,expected_generation}=>proxy.cache_purge(upstream,key,all,expected_generation).await,
            Command::RateTableList { listener, limit } => {
                proxy.rate_table_list(listener, limit).await
            }
            Command::RateTableShow { target, query } => proxy.rate_table_show(target, query).await,
            Command::RateTableClear {
                target,
                key,
                all,
                expected_generation,
            } => {
                proxy
                    .rate_table_clear(target, key, all, expected_generation)
                    .await
            }
            Command::FlushAccessLog {
                expected_generation,
            } => proxy.access_log_control(false, expected_generation).await,
            Command::ReopenAccessLog {
                expected_generation,
            } => proxy.access_log_control(true, expected_generation).await,
            Command::CollectArtifacts {
                dry_run,
                min_age_seconds,
                max_entries,
                max_delete_files,
                expected_generation,
            } => {
                let report = proxy
                    .collect_artifacts(
                        GcOptions {
                            dry_run,
                            min_age_seconds,
                            max_entries,
                            max_delete_files,
                        },
                        expected_generation,
                    )
                    .await?;
                Ok(report.json())
            }
            Command::Validate { config } => {
                proxy.validate(config).await?;
                Ok(json!({"valid":true}))
            }
            Command::Apply {
                config,
                expected_generation,
            } => proxy.apply(config, expected_generation).await,
            Command::ReloadTls { listener, tls, expected_generation } => {
                proxy.reload_tls(listener, *tls, expected_generation).await
            }
            Command::ReloadTlsMany { updates, expected_generation } => {
                proxy.reload_tls_many(updates, expected_generation).await
            }
            Command::SetEnabled {
                endpoint,
                enabled,
                expected_generation,
            } => {
                proxy
                    .set_enabled(endpoint, enabled, expected_generation)
                    .await
            }
        }
    }
    .await;
    let generation = proxy.snapshot().config.generation;
    match result {
        Ok(value) => Response {
            version: 1,
            id: request.id,
            generation: value
                .get("generation")
                .and_then(Value::as_u64)
                .unwrap_or(generation),
            result: Some(value),
            error: None,
        },
        Err(e) => {
            let message = format!("{e:#}");
            let code = if message.contains("generation_conflict") {
                "generation_conflict"
            } else if message.contains("unsupported_api_version") {
                "unsupported_api_version"
            } else if message.contains("worker_drain_timeout") {
                "worker_drain_timeout"
            } else if message.contains("generation_not_committed") {
                "generation_not_committed"
            } else if message.contains("too_many_worker_waiters") {
                "busy"
            } else {
                "invalid_or_unavailable"
            };
            Response {
                version: 1,
                id: request.id,
                generation,
                result: None,
                error: Some(ApiError {
                    code: code.into(),
                    message,
                }),
            }
        }
    }
}
