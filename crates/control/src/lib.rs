#![cfg(unix)]
use anyhow::{ensure, Context, Result};
use prost_core::RunningProxy;
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
    #[serde(rename = "upstream.set_enabled")]
    SetEnabled {
        endpoint: String,
        enabled: bool,
        expected_generation: u64,
    },
    Status,
    #[serde(rename = "config.export")]
    Export,
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
        tasks.spawn(async move {
            loop {
                let accepted=tokio::select!{_=token.cancelled()=>break,a=listener.accept()=>a};
                let Ok((mut io,_))=accepted else {continue};
                let Ok(permit)=capacity.clone().try_acquire_owned() else {continue};
                let p=proxy.clone();let token=token.clone();
                tracker.spawn(async move {
                    let _permit=permit;
                    let work=async {
                        let req:Request=tokio::time::timeout(Duration::from_secs(10),read_frame(&mut io)).await??;
                        let response=dispatch(p,req).await;
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
async fn dispatch(proxy: Arc<RunningProxy>, request: Request) -> Response {
    let result = async {
        ensure!(request.version == 1, "unsupported_api_version");
        match request.command {
            Command::Status => Ok(proxy.status()),
            Command::Export => Ok(json!({"config":proxy.snapshot().config.encode()?})),
            Command::Validate { config } => {
                proxy.validate(config).await?;
                Ok(json!({"valid":true}))
            }
            Command::Apply {
                config,
                expected_generation,
            } => proxy.apply(config, expected_generation).await,
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
            generation,
            result: Some(value),
            error: None,
        },
        Err(e) => {
            let message = format!("{e:#}");
            let code = if message.contains("generation_conflict") {
                "generation_conflict"
            } else if message.contains("unsupported_api_version") {
                "unsupported_api_version"
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
