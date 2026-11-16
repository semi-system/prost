//! Root-confined static files. Every component is opened relative to an owned
//! directory FD with O_NOFOLLOW, including index files; no check/open race.
use crate::{Body, Error, KeepAlive, Outcome};
use anyhow::{Context as _, Result};
use bytes::Bytes;
use futures_util::Stream;
use http::{header, HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    ffi::CString,
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::OpenOptionsExt,
    },
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::io::ReaderStream;
mod disk_cache;
mod ram;
pub use ram::RamBudget;

#[derive(serde::Serialize)]
pub struct StaticStats {
    pub mode: &'static str,
    pub entries: usize,
    pub reserved_bytes: u64,
    pub hits: u64,
    pub misses: u64,
}

pub struct StaticService {
    config: prost_config::StaticFiles,
    files: Arc<Semaphore>,
    ram: Option<ram::RamImage>,
    disk_cache: Option<Arc<disk_cache::DiskCache>>,
    ram_hits: AtomicU64,
    ram_misses: AtomicU64,
}
impl StaticService {
    pub fn new(config: &prost_config::StaticFiles, budget: &Arc<RamBudget>) -> Result<Self> {
        config.validate()?;
        let root = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&config.root)
            .context("open static root")?;
        let ram = if config.load_to_ram {
            Some(ram::RamImage::load(&root, config, budget)?)
        } else {
            None
        };
        let disk_cache = (!config.load_to_ram).then(|| disk_cache::DiskCache::new(config, budget));
        Ok(Self {
            config: config.clone(),
            files: Arc::new(Semaphore::new(config.max_open_files)),
            ram,
            disk_cache,
            ram_hits: AtomicU64::new(0),
            ram_misses: AtomicU64::new(0),
        })
    }
    pub fn stats(&self) -> StaticStats {
        if let Some(cache) = &self.disk_cache {
            return cache.stats();
        }
        let ram = self.ram.as_ref().unwrap();
        StaticStats {
            mode: "ram",
            entries: ram.len(),
            reserved_bytes: ram.reserved_bytes(),
            hits: self.ram_hits.load(Ordering::Relaxed),
            misses: self.ram_misses.load(Ordering::Relaxed),
        }
    }

    pub async fn serve<B: Send + Sync>(
        &self,
        request: &Request<B>,
        prefix: &str,
        permit: OwnedSemaphorePermit,
        keepalive: KeepAlive,
        notify: bool,
    ) -> Result<Outcome> {
        let mut response = crate::response(200, "");
        if request.method() != Method::GET && request.method() != Method::HEAD {
            *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(local(response, permit, keepalive, notify));
        }
        let path = if self.config.strip_prefix {
            request
                .uri()
                .path()
                .strip_prefix(prefix)
                .unwrap_or(request.uri().path())
        } else {
            request.uri().path()
        };
        let parts = match components(path) {
            Ok(parts) => parts,
            Err(()) => {
                return Ok(local(
                    crate::response(403, "forbidden path\n"),
                    permit,
                    keepalive,
                    notify,
                ))
            }
        };
        let Ok(file_slot) = self.files.clone().try_acquire_owned() else {
            return Ok(local(
                crate::response(503, "static file capacity\n"),
                permit,
                keepalive,
                notify,
            ));
        };
        let root = self.config.root.clone();
        let indexes = self.config.index.clone();
        let trailing_slash = request.uri().path().ends_with('/');
        let opened = if let Some(ram) = &self.ram {
            let found = ram.open(&parts, &indexes, trailing_slash);
            if found.is_ok() {
                self.ram_hits.fetch_add(1, Ordering::Relaxed);
            } else {
                self.ram_misses.fetch_add(1, Ordering::Relaxed);
            }
            found.map(|f| Opened {
                source: Source::Ram(f.content),
                name: f.name,
                redirect: f.redirect,
                size: f.size,
                modified: f.modified,
                cache: None,
                identity: f.identity,
            })
        } else {
            tokio::task::spawn_blocking(move || -> io::Result<(File, String, bool, String)> {
                // Reopen the configured root so an atomic deployment replacing
                // the directory cannot pin the disk cache to an obsolete tree.
                let root = std::fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(root)?;
                let path = parts.join("/");
                let mut file = open_beneath(&root, &parts)?;
                let directory = file.metadata()?.is_dir();
                if directory && !trailing_slash {
                    return Ok((file, String::new(), true, path));
                }
                if directory {
                    for name in indexes {
                        match open_beneath(&file, std::slice::from_ref(&name)) {
                            Ok(index) if index.metadata()?.is_file() => {
                                file = index;
                                let key = if path.is_empty() {
                                    name.clone()
                                } else {
                                    format!("{path}/{name}")
                                };
                                return Ok((file, name, false, key));
                            }
                            Ok(_) => {}
                            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                            Err(e) => return Err(e),
                        }
                    }
                    return Err(io::Error::from(io::ErrorKind::PermissionDenied));
                }
                if !file.metadata()?.is_file() {
                    return Err(io::Error::from(io::ErrorKind::PermissionDenied));
                }
                Ok((file, parts.last().cloned().unwrap_or_default(), false, path))
            })
            .await?
            .and_then(|(file, name, redirect, key)| {
                let metadata = file.metadata()?;
                Ok(Opened {
                    source: Source::File(file),
                    name,
                    redirect,
                    size: metadata.len(),
                    modified: metadata.modified().unwrap_or(UNIX_EPOCH),
                    cache: Some((key, disk_cache::Stamp::new(&metadata))),
                    identity: ram::identity(&metadata),
                })
            })
        };
        let Opened {
            mut source,
            name,
            redirect,
            size,
            modified,
            cache,
            identity,
        } = match opened {
            Ok(opened) => opened,
            Err(e) => {
                let status = match e.kind() {
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => 404,
                    io::ErrorKind::PermissionDenied => 403,
                    _ if e.raw_os_error() == Some(libc::ELOOP) => 403,
                    _ => return Err(e.into()),
                };
                return Ok(local(
                    crate::response(status, "file unavailable\n"),
                    permit,
                    keepalive,
                    notify,
                ));
            }
        };
        if redirect {
            *response.status_mut() = StatusCode::MOVED_PERMANENTLY;
            let mut location = format!("{}/", request.uri().path());
            if let Some(query) = request.uri().query() {
                location.push('?');
                location.push_str(query);
            }
            // Keep the redirect relative; a path starting // must not become an
            // attacker-selected authority in a Location field.
            if location.starts_with("//") {
                location.insert(0, '.');
            }
            response
                .headers_mut()
                .insert(header::LOCATION, HeaderValue::from_str(&location)?);
            return Ok(local(response, permit, keepalive, notify));
        }
        if let (Some(disk), Some((key, stamp))) = (&self.disk_cache, &cache) {
            if let Some(data) = disk.lookup(key, stamp) {
                source = Source::Ram(data);
            }
        }
        let etag = format!(
            "W/\"{size:x}-{:x}-{:x}-{:x}-{:x}-{:x}-{:x}\"",
            identity[0], identity[1], identity[2], identity[3], identity[4], identity[5]
        );
        response
            .headers_mut()
            .insert(header::ETAG, HeaderValue::from_str(&etag)?);
        response.headers_mut().insert(
            header::LAST_MODIFIED,
            HeaderValue::from_str(&httpdate::fmt_http_date(modified))?,
        );
        response
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(mime(&name)));
        response.headers_mut().insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        // Our metadata validator is weak, so it cannot satisfy a strong
        // If-Match comparison. '*' still matches an existing resource.
        let if_match = request.headers().get(header::IF_MATCH);
        let precondition_failed = if let Some(value) = if_match {
            !value
                .to_str()
                .is_ok_and(|v| v.split(',').any(|tag| tag.trim() == "*"))
        } else {
            request
                .headers()
                .get(header::IF_UNMODIFIED_SINCE)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| httpdate::parse_http_date(v).ok())
                .is_some_and(|date| seconds(modified) > seconds(date))
        };
        if precondition_failed {
            *response.status_mut() = StatusCode::PRECONDITION_FAILED;
            return Ok(local(response, permit, keepalive, notify));
        }
        if not_modified(request.headers(), &etag, modified) {
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            return Ok(local(response, permit, keepalive, notify));
        }
        let range = request
            .headers()
            .get(header::RANGE)
            .filter(|_| {
                request.method() == Method::GET
                    && request.headers().get(header::IF_RANGE).is_none_or(|v| {
                        v.to_str()
                            .ok()
                            .and_then(|v| httpdate::parse_http_date(v).ok())
                            .is_some_and(|date| seconds(modified) <= seconds(date))
                    })
            })
            .filter(|value| {
                value
                    .to_str()
                    .is_ok_and(|v| v.starts_with("bytes=") && !v.contains(','))
            });
        let (offset, length) = if let Some(value) = range {
            match value.to_str().ok().and_then(|v| byte_range(v, size)) {
                Some(range) => {
                    *response.status_mut() = StatusCode::PARTIAL_CONTENT;
                    response.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!(
                            "bytes {}-{}/{}",
                            range.0,
                            range.0 + range.1 - 1,
                            size
                        ))?,
                    );
                    range
                }
                None => {
                    *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
                    response.headers_mut().insert(
                        header::CONTENT_RANGE,
                        HeaderValue::from_str(&format!("bytes */{size}"))?,
                    );
                    return Ok(local(response, permit, keepalive, notify));
                }
            }
        } else {
            (0, size)
        };
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&length.to_string())?,
        );
        if request.method() == Method::HEAD || length == 0 {
            return Ok(local(response, permit, keepalive, notify));
        }
        let file = match source {
            Source::File(file) => file,
            Source::Ram(data) => {
                *response.body_mut() =
                    Body::bytes(data.slice(offset as usize..(offset + length) as usize));
                return Ok(local(response, permit, keepalive, notify));
            }
        };
        let mut file = tokio::fs::File::from_std(file);
        if offset == 0 && length == size {
            if let (Some(disk), Some((key, stamp))) = (&self.disk_cache, &cache) {
                if let Some(data) = disk.read(&mut file, key, stamp).await? {
                    *response.body_mut() = Body::bytes(data);
                    return Ok(local(response, permit, keepalive, notify));
                }
            }
        }
        file.seek(io::SeekFrom::Start(offset)).await?;
        let stream = FileBody {
            reader: ReaderStream::with_capacity(file.take(length), 16 * 1024),
            remaining: length,
            _slot: file_slot,
        };
        let (body, completion) = Body::local_stream(stream, permit, keepalive, notify);
        Ok(Outcome {
            response: response.map(|_| body),
            tunnel: None,
            completion,
        })
    }
}
enum Source {
    File(File),
    Ram(Bytes),
}
struct Opened {
    source: Source,
    name: String,
    redirect: bool,
    size: u64,
    modified: SystemTime,
    cache: Option<(String, disk_cache::Stamp)>,
    identity: [i128; 6],
}

fn local(
    response: Response<Body>,
    permit: OwnedSemaphorePermit,
    keepalive: KeepAlive,
    notify: bool,
) -> Outcome {
    let (parts, body) = response.into_parts();
    let (body, completion) = Body::local_stream(body, permit, keepalive, notify);
    Outcome {
        response: Response::from_parts(parts, body),
        tunnel: None,
        completion,
    }
}
fn components(path: &str) -> std::result::Result<Vec<String>, ()> {
    let mut result = Vec::new();
    for part in path.split('/').filter(|s| !s.is_empty()) {
        let decoded = percent_encoding::percent_decode_str(part)
            .decode_utf8()
            .map_err(|_| ())?;
        if decoded == "." || decoded == ".." || decoded.contains(['/', '\\', '\0']) {
            return Err(());
        }
        result.push(decoded.into_owned());
    }
    Ok(result)
}
fn open_beneath(root: &File, parts: &[String]) -> io::Result<File> {
    let mut file = root.try_clone()?;
    for (i, part) in parts.iter().enumerate() {
        let name = CString::new(part.as_bytes()).map_err(|_| io::ErrorKind::InvalidInput)?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if i + 1 < parts.len() {
                libc::O_DIRECTORY
            } else {
                0
            };
        // name is NUL-terminated; file owns a valid FD for this entire call.
        let fd = unsafe { libc::openat(file.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // openat returned a new, exclusively owned descriptor.
        file = unsafe { File::from_raw_fd(fd) };
    }
    Ok(file)
}
fn seconds(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn not_modified(headers: &HeaderMap, etag: &str, modified: SystemTime) -> bool {
    if headers.contains_key(header::IF_NONE_MATCH) {
        return headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
            value.to_str().is_ok_and(|v| {
                v.split(',').any(|tag| {
                    let tag = tag.trim();
                    tag == "*" || tag.trim_start_matches("W/") == etag.trim_start_matches("W/")
                })
            })
        });
    }
    headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| httpdate::parse_http_date(v).ok())
        .is_some_and(|date| seconds(modified) <= seconds(date))
}
fn byte_range(value: &str, size: u64) -> Option<(u64, u64)> {
    let value = value.strip_prefix("bytes=")?;
    let (start, end) = value.split_once('-')?;
    if size == 0 || value.contains(',') {
        return None;
    }
    if start.is_empty() {
        let length = end.parse::<u64>().ok()?.min(size);
        return (length > 0).then_some((size - length, length));
    }
    let start = start.parse::<u64>().ok()?;
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().ok()?.min(size - 1)
    };
    (start <= end && start < size).then(|| (start, end - start + 1))
}
fn mime(name: &str) -> &'static str {
    match name
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "wasm" => "application/wasm",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "zip" => "application/zip",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "mp3" => "audio/mpeg",
        _ => "application/octet-stream",
    }
}
struct FileBody {
    reader: ReaderStream<tokio::io::Take<tokio::fs::File>>,
    remaining: u64,
    _slot: OwnedSemaphorePermit,
}
impl HttpBody for FileBody {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        if self.remaining == 0 {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.reader).poll_next(cx) {
            Poll::Ready(Some(Ok(data))) => {
                self.remaining -= data.len() as u64;
                Poll::Ready(Some(Ok(Frame::data(data))))
            }
            Poll::Ready(Some(Err(e))) => {
                self.remaining = 0;
                Poll::Ready(Some(Err(e.into())))
            }
            Poll::Ready(None) => {
                self.remaining = 0;
                Poll::Ready(Some(Err(
                    io::Error::from(io::ErrorKind::UnexpectedEof).into()
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.remaining)
    }
}
