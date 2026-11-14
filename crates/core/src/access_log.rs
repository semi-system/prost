use anyhow::{ensure, Context, Result};
use prost_config::AccessLog;
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::{BufWriter, Write},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

#[derive(Default)]
pub(crate) struct Counters {
    pub filtered: AtomicU64,
    pub accepted: AtomicU64,
    pub active: AtomicU64,
    pub processed: AtomicU64,
    pub sampled_out: AtomicU64,
    pub pending: AtomicU64,
    pub dropped: [AtomicU64; 3], // queue full, uncertain IO, closed
    pub io_errors: AtomicU64,
    pub reopens: AtomicU64,
    pub flushes: AtomicU64,
    pub alive: AtomicU64,
}
#[derive(Serialize)]
pub(crate) struct Record {
    schema_version: u8,
    timestamp_unix_ms: u64,
    protocol: &'static str,
    peer: String,
    listener: String,
    generation: u64,
    method: Option<String>,
    http_version: Option<String>,
    path: Option<String>,
    truncated: bool,
    status: Option<u16>,
    upstream: Option<String>,
    endpoint: Option<String>,
    body_bytes: Option<u64>,
    headers_us: Option<u64>,
    duration_us: u64,
    outcome: &'static str,
}
struct Queued {
    record: Option<Record>,
    counters: Arc<Counters>,
}
impl Queued {
    fn take(&mut self) -> Record {
        self.counters.pending.fetch_sub(1, Ordering::Relaxed);
        self.record.take().unwrap()
    }
    fn discard(mut self, reason: usize) {
        self.record.take();
        self.counters.pending.fetch_sub(1, Ordering::Relaxed);
        self.counters.dropped[reason].fetch_add(1, Ordering::Relaxed);
    }
}
impl Drop for Queued {
    fn drop(&mut self) {
        if self.record.is_some() {
            self.counters.pending.fetch_sub(1, Ordering::Relaxed);
            self.counters.dropped[2].fetch_add(1, Ordering::Relaxed);
        }
    }
}
type Reply = oneshot::Sender<std::result::Result<u64, String>>;
// Bounded inline records avoid another allocation for every accepted log line.
#[allow(clippy::large_enum_variant)]
enum Event {
    Record(Queued),
    Flush(Reply),
    Reopen(File, Reply),
    Stop(Reply),
}
pub(crate) struct Logger {
    sender: SyncSender<Event>,
    pub config: AccessLog,
    path: PathBuf,
    managed_config: PathBuf,
    artifacts: PathBuf,
    pub counters: Arc<Counters>,
    closed: AtomicBool,
    sequence: AtomicU64,
}
fn open_file(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .append(true)
        .create(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.nlink() == 1,
        "access log must be a regular file without hard links"
    );
    Ok(file)
}
impl Logger {
    pub fn path_enabled(&self, path: &str) -> bool {
        let yes = (self.config.include_paths.is_empty()
            || self
                .config
                .include_paths
                .iter()
                .any(|p| path.starts_with(p)))
            && !self
                .config
                .exclude_paths
                .iter()
                .any(|p| path.starts_with(p));
        if !yes {
            self.counters.filtered.fetch_add(1, Ordering::Relaxed);
        }
        yes
    }
    pub fn status(&self) -> serde_json::Value {
        let c = &self.counters;
        serde_json::json!({"enabled":true,"writer_alive":c.alive.load(Ordering::Acquire),"accepted":c.accepted.load(Ordering::Relaxed),"processed":c.processed.load(Ordering::Relaxed),"pending":c.pending.load(Ordering::Relaxed),"active":c.active.load(Ordering::Relaxed),"sampled_out":c.sampled_out.load(Ordering::Relaxed),"dropped_queue_full":c.dropped[0].load(Ordering::Relaxed),"dropped_io_uncertain":c.dropped[1].load(Ordering::Relaxed),"dropped_closed":c.dropped[2].load(Ordering::Relaxed),"io_errors":c.io_errors.load(Ordering::Relaxed),"reopens":c.reopens.load(Ordering::Relaxed),"flushes":c.flushes.load(Ordering::Relaxed)})
    }
    pub fn start(config: &AccessLog, managed_config: &Path, artifacts: &Path) -> Result<Arc<Self>> {
        let parent = std::fs::canonicalize(
            config
                .path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?;
        let path = parent.join(config.path.file_name().context("access log filename")?);
        ensure!(
            !parent.starts_with(artifacts)
                && path != managed_config
                && path != managed_config.with_extension("toml.lock"),
            "access log cannot overwrite config, locks or managed artifacts"
        );
        let file =
            open_file(&path).with_context(|| format!("open access log {}", path.display()))?;
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let counters = Arc::new(Counters::default());
        counters.alive.store(1, Ordering::Relaxed);
        let stats = counters.clone();
        let flush = Duration::from_millis(config.flush_interval_ms);
        let format = config.format;
        std::thread::Builder::new()
            .name("prost-access-log".into())
            .spawn(move || writer_loop_format(file, receiver, stats, flush, format))?;
        Ok(Arc::new(Self {
            sender,
            config: config.clone(),
            path,
            managed_config: managed_config.to_owned(),
            artifacts: artifacts.to_owned(),
            counters,
            closed: AtomicBool::new(false),
            sequence: AtomicU64::new(0),
        }))
    }
    pub fn sample(&self) -> bool {
        if self
            .sequence
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(self.config.sample_every)
        {
            true
        } else {
            self.counters.sampled_out.fetch_add(1, Ordering::Relaxed);
            false
        }
    }
    fn emit(&self, record: Record) {
        if !self.config.statuses.is_empty()
            && !record
                .status
                .is_some_and(|s| self.config.statuses.contains(&s))
        {
            self.counters.filtered.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.closed.load(Ordering::Acquire) {
            self.counters.dropped[2].fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.counters.pending.fetch_add(1, Ordering::Relaxed);
        let queued = Queued {
            record: Some(record),
            counters: self.counters.clone(),
        };
        match self.sender.try_send(Event::Record(queued)) {
            Ok(()) => {
                self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(Event::Record(record))) => record.discard(0),
            Err(TrySendError::Disconnected(Event::Record(record))) => record.discard(2),
            _ => unreachable!(),
        }
    }
    pub async fn control(&self, reopen: bool) -> Result<u64> {
        ensure!(
            !self.closed.load(Ordering::Acquire),
            "access log is shutting down"
        );
        let (tx, rx) = oneshot::channel();
        let event = if reopen {
            let path = self.path.clone();
            let config = self.managed_config.clone();
            let artifacts = self.artifacts.clone();
            Event::Reopen(
                tokio::task::spawn_blocking(move || {
                    let parent =
                        std::fs::canonicalize(path.parent().context("access log parent")?)?;
                    let resolved = parent.join(path.file_name().context("access log filename")?);
                    ensure!(
                        !parent.starts_with(&artifacts)
                            && resolved != config
                            && resolved != config.with_extension("toml.lock"),
                        "access log cannot overwrite managed files"
                    );
                    open_file(&resolved)
                })
                .await??,
                tx,
            )
        } else {
            Event::Flush(tx)
        };
        self.command(event, rx).await
    }
    async fn command(
        &self,
        mut event: Event,
        rx: oneshot::Receiver<std::result::Result<u64, String>>,
    ) -> Result<u64> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match self.sender.try_send(event) {
                    Ok(()) => break,
                    Err(TrySendError::Full(value)) => {
                        event = value;
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(TrySendError::Disconnected(_)) => {
                        anyhow::bail!("access log writer stopped")
                    }
                }
            }
            rx.await
                .context("access log writer stopped")?
                .map_err(anyhow::Error::msg)
        })
        .await
        .context("access log control timeout; check metrics before retrying")?
    }
    pub async fn shutdown(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.command(Event::Stop(tx), rx).await?;
        Ok(())
    }
}
struct Writer {
    file: Option<BufWriter<File>>,
    buffered: u64,
    counters: Arc<Counters>,
}
impl Writer {
    fn failure(&mut self, current: u64) {
        self.counters.io_errors.fetch_add(1, Ordering::Relaxed);
        self.counters.dropped[1]
            .fetch_add(self.buffered.saturating_add(current), Ordering::Relaxed);
        self.buffered = 0;
        if let Some(file) = self.file.take() {
            let _ = file.into_parts();
        }
        tracing::warn!("access log IO failure; logging suspended until reopen");
    }
    fn flush(&mut self) -> std::result::Result<u64, String> {
        let result = self
            .file
            .as_mut()
            .ok_or_else(|| "access log sink unavailable".to_owned())?
            .flush();
        if let Err(error) = result {
            self.failure(0);
            return Err(error.to_string());
        }
        self.buffered = 0;
        self.counters.flushes.fetch_add(1, Ordering::Relaxed);
        Ok(self.counters.reopens.load(Ordering::Relaxed))
    }
}
#[cfg(test)]
fn writer_loop(file: File, receiver: Receiver<Event>, counters: Arc<Counters>, interval: Duration) {
    writer_loop_format(
        file,
        receiver,
        counters,
        interval,
        prost_config::LogFormat::Json,
    );
}
fn writer_loop_format(
    file: File,
    receiver: Receiver<Event>,
    counters: Arc<Counters>,
    interval: Duration,
    format: prost_config::LogFormat,
) {
    struct Alive(Arc<Counters>);
    impl Drop for Alive {
        fn drop(&mut self) {
            self.0.alive.store(0, Ordering::Release);
        }
    }
    let _alive = Alive(counters.clone());
    let mut writer = Writer {
        file: Some(BufWriter::with_capacity(65536, file)),
        buffered: 0,
        counters: counters.clone(),
    };
    let mut line = Vec::with_capacity(2048);
    let mut flushed = Instant::now();
    loop {
        match receiver.recv_timeout(interval.saturating_sub(flushed.elapsed())) {
            Ok(Event::Record(mut queued)) => {
                let record = queued.take();
                counters.processed.fetch_add(1, Ordering::Relaxed);
                if writer.file.is_none() {
                    counters.dropped[1].fetch_add(1, Ordering::Relaxed);
                } else {
                    line.clear();
                    #[derive(Serialize)]
                    struct Compact<'a> {
                        t: u64,
                        g: u64,
                        p: &'a str,
                        r: &'a str,
                        l: &'a str,
                        m: Option<&'a str>,
                        u: Option<&'a str>,
                        s: Option<u16>,
                        b: Option<u64>,
                        d: u64,
                        o: &'a str,
                    }
                    let encoded = match format {
                        prost_config::LogFormat::Json => serde_json::to_writer(&mut line, &record),
                        prost_config::LogFormat::Compact => serde_json::to_writer(
                            &mut line,
                            &Compact {
                                t: record.timestamp_unix_ms,
                                g: record.generation,
                                p: record.protocol,
                                r: &record.peer,
                                l: &record.listener,
                                m: record.method.as_deref(),
                                u: record.path.as_deref(),
                                s: record.status,
                                b: record.body_bytes,
                                d: record.duration_us,
                                o: record.outcome,
                            },
                        ),
                    };
                    if encoded.is_err() {
                        writer.failure(1);
                    } else {
                        line.push(b'\n');
                        if writer.file.as_mut().unwrap().write_all(&line).is_err() {
                            writer.failure(1);
                        } else {
                            writer.buffered += 1;
                        }
                    }
                }
            }
            Ok(Event::Flush(reply)) => {
                let _ = reply.send(writer.flush());
                flushed = Instant::now();
            }
            Ok(Event::Reopen(file, reply)) => {
                let _ = writer.flush();
                writer.file = Some(BufWriter::with_capacity(65536, file));
                writer.buffered = 0;
                let epoch = counters.reopens.fetch_add(1, Ordering::Relaxed) + 1;
                let _ = reply.send(Ok(epoch));
                flushed = Instant::now();
            }
            Ok(Event::Stop(reply)) => {
                let _ = reply.send(writer.flush());
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let _ = writer.flush();
                flushed = Instant::now();
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                let _ = writer.flush();
                break;
            }
        }
        if flushed.elapsed() >= interval {
            let _ = writer.flush();
            flushed = Instant::now();
        }
    }
}

fn clip(value: &str, limit: usize) -> (String, bool) {
    let mut end = value.len().min(limit);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (value[..end].to_owned(), end < value.len())
}
fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}
struct State {
    record: Option<Record>,
    headers: bool,
    finish: Option<(Option<u64>, &'static str, u64)>,
}
pub(crate) struct Ticket {
    logger: Arc<Logger>,
    started: Instant,
    state: Mutex<State>,
    tunnel: AtomicBool,
}
impl Ticket {
    pub fn http(
        logger: Arc<Logger>,
        peer: &str,
        listener: &str,
        generation: u64,
        method: &str,
        version: String,
        path: &str,
    ) -> Arc<Self> {
        Self::new(
            logger,
            "http",
            peer,
            listener,
            generation,
            Some((method, version, path)),
        )
    }
    pub fn tcp(logger: Arc<Logger>, peer: &str, listener: &str, generation: u64) -> Arc<Self> {
        Self::new(logger, "tcp", peer, listener, generation, None)
    }
    fn new(
        logger: Arc<Logger>,
        protocol: &'static str,
        peer: &str,
        listener: &str,
        generation: u64,
        http: Option<(&str, String, &str)>,
    ) -> Arc<Self> {
        let (method, version, path, truncated) = match http {
            Some((method, version, path)) => {
                let (method, a) = clip(method, 32);
                let (path, b) = clip(path, logger.config.max_path_bytes);
                (Some(method), Some(version), Some(path), a || b)
            }
            None => (None, None, None, false),
        };
        let record = Record {
            schema_version: 1,
            timestamp_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            protocol,
            peer: clip(peer, 128).0,
            listener: clip(listener, 128).0,
            generation,
            method,
            http_version: version,
            path,
            truncated,
            status: None,
            upstream: None,
            endpoint: None,
            body_bytes: None,
            headers_us: None,
            duration_us: 0,
            outcome: "aborted",
        };
        logger.counters.active.fetch_add(1, Ordering::Relaxed);
        Arc::new(Self {
            logger,
            started: Instant::now(),
            state: Mutex::new(State {
                record: Some(record),
                headers: protocol == "tcp",
                finish: None,
            }),
            tunnel: AtomicBool::new(false),
        })
    }
    pub fn upstream(&self, name: Option<&str>, endpoint: Option<&str>) {
        let mut s = self.state.lock().unwrap();
        if let Some(record) = &mut s.record {
            record.upstream = name.map(|s| clip(s, 128).0);
            record.endpoint = endpoint.map(|s| clip(s, 512).0);
        }
    }
    pub fn peer(&self, peer: &str) {
        if let Some(record) = &mut self.state.lock().unwrap().record {
            record.peer = clip(peer, 128).0;
        }
    }
    pub fn arm_tunnel(&self) {
        self.tunnel.store(true, Ordering::Release);
    }
    pub fn is_tunnel(&self) -> bool {
        self.tunnel.load(Ordering::Acquire)
    }
    pub fn headers(&self, status: u16) {
        let mut s = self.state.lock().unwrap();
        if let Some(record) = &mut s.record {
            record.status = Some(status);
            record.headers_us = Some(micros(self.started));
            if self.is_tunnel() {
                record.protocol = "websocket";
            }
        }
        s.headers = true;
        self.emit_ready(&mut s);
    }
    pub fn finish(&self, bytes: Option<u64>, outcome: &'static str) {
        let mut s = self.state.lock().unwrap();
        if s.finish.is_none() {
            s.finish = Some((bytes, outcome, micros(self.started)));
        }
        self.emit_ready(&mut s);
    }
    fn emit_ready(&self, state: &mut State) {
        if !state.headers {
            return;
        }
        if let Some((bytes, outcome, duration)) = state.finish {
            if let Some(mut record) = state.record.take() {
                record.body_bytes = bytes;
                record.outcome = outcome;
                record.duration_us = duration.max(record.headers_us.unwrap_or(0));
                self.logger.emit(record);
            }
        }
    }
}
impl prost_http::BodyObserver for Ticket {
    fn end(&self, bytes: u64, result: prost_http::BodyEnd) {
        self.finish(
            Some(bytes),
            match result {
                prost_http::BodyEnd::Complete => "complete",
                prost_http::BodyEnd::Error => "error",
                prost_http::BodyEnd::Aborted => "aborted",
            },
        );
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.logger.counters.active.fetch_sub(1, Ordering::Relaxed);
        let state = self.state.get_mut().unwrap_or_else(|e| e.into_inner());
        if let Some(mut record) = state.record.take() {
            record.duration_us = micros(self.started);
            record.outcome = "aborted";
            self.logger.emit(record);
        }
    }
}

pub(crate) struct RequestContext {
    pub snapshot: Arc<super::Snapshot>,
    pub route: Option<usize>,
    pub audit: Option<Arc<Ticket>>,
}

#[cfg(test)]
mod tests;
