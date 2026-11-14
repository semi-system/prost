//! Opt-in HTTP/1 header/idle clock through public Hyper Timer interfaces.
//! A bounded framing observer detects already-buffered pipelined prefixes.
//! Hyper remains the authority for parsing/accepting traffic; uncertain framing
//! falls back to the shorter header deadline, never to a longer idle deadline.
use prost_transport::BoxStream;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Shared {
    idle_expired: AtomicBool,
    idle_wake: tokio::sync::Notify,
    origin: tokio::time::Instant,
    output_ms: AtomicU64,
    write_pending: AtomicBool,
    frontier: AtomicU64,
    ordinal: AtomicU64,
    uncertain: AtomicBool,
}
impl Default for Shared {
    fn default() -> Self {
        Self {
            idle_expired: AtomicBool::new(false),
            idle_wake: tokio::sync::Notify::new(),
            origin: tokio::time::Instant::now(),
            output_ms: AtomicU64::new(0),
            write_pending: AtomicBool::new(false),
            frontier: AtomicU64::new(0),
            ordinal: AtomicU64::new(0),
            uncertain: AtomicBool::new(false),
        }
    }
}
#[derive(Clone)]
pub struct HeaderClock {
    shared: Arc<Shared>,
    header: Option<Duration>,
    idle: Duration,
}
impl HeaderClock {
    pub async fn expired(&self) {
        loop {
            let notified = self.shared.idle_wake.notified();
            if self.shared.idle_expired.load(Ordering::Relaxed) {
                return;
            }
            notified.await;
        }
    }
    pub fn wrap(io: BoxStream, header_ms: u64, idle_ms: u64) -> (BoxStream, Self) {
        let shared = Arc::new(Shared::default());
        let clock = Self {
            shared: shared.clone(),
            header: (header_ms > 0).then(|| Duration::from_millis(header_ms)),
            idle: Duration::from_millis(idle_ms),
        };
        (
            Box::new(Observed {
                io,
                dirty: false,
                scanner: Scanner {
                    shared,
                    state: State::Head,
                    buffer: Vec::with_capacity(512),
                    trailers: 0,
                },
            }),
            clock,
        )
    }
    fn head_sleep(&self) -> HeadSleep {
        let ordinal = self.shared.ordinal.fetch_add(1, Ordering::Relaxed) + 1;
        let head = ordinal == 1
            || self.shared.uncertain.load(Ordering::Relaxed)
            || self.shared.frontier.load(Ordering::Relaxed) >= ordinal;
        let budget = if head { self.header } else { Some(self.idle) };
        HeadSleep {
            created: tokio::time::Instant::now(),
            clock: self.clone(),
            ordinal,
            head,
            timer: budget.map(|d| Box::pin(tokio::time::sleep(d))),
        }
    }
}
impl hyper::rt::Timer for HeaderClock {
    fn sleep(&self, _: Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(self.head_sleep())
    }
    fn sleep_until(&self, _: Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        Box::pin(self.head_sleep())
    }
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}
struct HeadSleep {
    created: tokio::time::Instant,
    clock: HeaderClock,
    ordinal: u64,
    head: bool,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}
impl hyper::rt::Sleep for HeadSleep {}
impl Future for HeadSleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.head
            && (self.clock.shared.uncertain.load(Ordering::Relaxed)
                || self.clock.shared.frontier.load(Ordering::Relaxed) >= self.ordinal)
        {
            self.head = true;
            self.timer = self.clock.header.map(|d| Box::pin(tokio::time::sleep(d)));
        }
        if !self.head {
            let output = self.clock.shared.origin
                + Duration::from_millis(self.clock.shared.output_ms.load(Ordering::Relaxed));
            let mut deadline = self.created.max(output) + self.clock.idle;
            if self.clock.shared.write_pending.load(Ordering::Relaxed) {
                deadline = deadline.max(tokio::time::Instant::now() + self.clock.idle);
            }
            if let Some(timer) = &mut self.timer {
                if timer.deadline() != deadline {
                    timer.as_mut().reset(deadline);
                }
            }
        }
        let result = self
            .timer
            .as_mut()
            .map_or(Poll::Pending, |timer| timer.as_mut().poll(cx));
        if !self.head && result.is_ready() {
            if !self.clock.shared.idle_expired.swap(true, Ordering::Relaxed) {
                self.clock.shared.idle_wake.notify_one();
            }
            // Let the owner shut down quietly; a Hyper header timeout would
            // otherwise synthesize an unsolicited HTTP 408 on an idle socket.
            Poll::Pending
        } else {
            result
        }
    }
}
#[derive(Clone, Copy)]
enum State {
    Head,
    Fixed(u64),
    ChunkLine,
    Chunk(u64),
    ChunkEnd(u8),
    Trailers,
    Unknown,
}
struct Scanner {
    shared: Arc<Shared>,
    state: State,
    buffer: Vec<u8>,
    trailers: usize,
}
impl Scanner {
    fn unknown(&mut self) {
        self.state = State::Unknown;
        self.buffer.clear();
        self.shared.uncertain.store(true, Ordering::Relaxed);
    }
    fn head(&self) -> Option<State> {
        let mut headers = [httparse::EMPTY_HEADER; 100];
        let mut request = httparse::Request::new(&mut headers);
        if !request.parse(&self.buffer).ok()?.is_complete() {
            return None;
        }
        let mut length = None;
        let mut chunked = false;
        for header in request.headers.iter() {
            if header.name.eq_ignore_ascii_case("upgrade") || request.method == Some("CONNECT") {
                return None;
            }
            if header.name.eq_ignore_ascii_case("content-length") {
                let text = std::str::from_utf8(header.value).ok()?.trim();
                if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let n = text.parse::<u64>().ok()?;
                if length.is_some_and(|old| old != n) {
                    return None;
                }
                length = Some(n);
            }
            if header.name.eq_ignore_ascii_case("transfer-encoding") {
                if chunked
                    || !std::str::from_utf8(header.value)
                        .ok()?
                        .trim()
                        .eq_ignore_ascii_case("chunked")
                {
                    return None;
                }
                chunked = true;
            }
        }
        if chunked {
            if length.is_some() {
                None
            } else {
                Some(State::ChunkLine)
            }
        } else {
            Some(match length.unwrap_or(0) {
                0 => State::Head,
                n => State::Fixed(n),
            })
        }
    }
    fn feed(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            match self.state {
                State::Unknown => return,
                State::Fixed(left) | State::Chunk(left) => {
                    let take = left.min(bytes.len() as u64);
                    bytes = &bytes[take as usize..];
                    self.state = if take == left {
                        match self.state {
                            State::Fixed(_) => State::Head,
                            _ => State::ChunkEnd(0),
                        }
                    } else {
                        match self.state {
                            State::Fixed(_) => State::Fixed(left - take),
                            _ => State::Chunk(left - take),
                        }
                    };
                }
                State::ChunkEnd(n) => {
                    if bytes[0] != [b'\r', b'\n'][n as usize] {
                        self.unknown();
                        return;
                    }
                    bytes = &bytes[1..];
                    self.state = if n == 0 {
                        State::ChunkEnd(1)
                    } else {
                        State::ChunkLine
                    };
                }
                state => {
                    if matches!(state, State::Head) && self.buffer.is_empty() {
                        self.shared.frontier.fetch_add(1, Ordering::Relaxed);
                    }
                    let limit = if matches!(state, State::ChunkLine) {
                        1024
                    } else {
                        32768
                    };
                    if self.buffer.len() >= limit {
                        self.unknown();
                        return;
                    }
                    self.buffer.push(bytes[0]);
                    bytes = &bytes[1..];
                    match state {
                        State::Head if self.buffer.ends_with(b"\r\n\r\n") => {
                            if let Some(next) = self.head() {
                                self.state = next;
                                self.buffer.clear();
                            } else {
                                self.unknown();
                            }
                        }
                        State::ChunkLine if self.buffer.ends_with(b"\r\n") => {
                            match httparse::parse_chunk_size(&self.buffer) {
                                Ok(httparse::Status::Complete((used, size)))
                                    if used == self.buffer.len() =>
                                {
                                    self.state = if size == 0 {
                                        self.trailers = 0;
                                        State::Trailers
                                    } else {
                                        State::Chunk(size)
                                    };
                                    self.buffer.clear();
                                }
                                _ => self.unknown(),
                            }
                        }
                        State::Trailers => {
                            self.trailers += 1;
                            if self.trailers > 32768 {
                                self.unknown();
                                return;
                            }
                            if self.buffer.ends_with(b"\r\n") {
                                if self.buffer.len() == 2 {
                                    self.state = State::Head;
                                }
                                self.buffer.clear();
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
}
struct Observed {
    io: BoxStream,
    dirty: bool,
    scanner: Scanner,
}
impl Observed {
    fn written(&mut self, result: &Poll<std::io::Result<usize>>, offered: usize) {
        match result {
            Poll::Ready(Ok(n)) if *n > 0 => {
                self.dirty = true;
                self.scanner.shared.output_ms.store(
                    self.scanner
                        .shared
                        .origin
                        .elapsed()
                        .as_millis()
                        .min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                self.scanner
                    .shared
                    .write_pending
                    .store(*n < offered, Ordering::Relaxed);
            }
            Poll::Pending => self
                .scanner
                .shared
                .write_pending
                .store(true, Ordering::Relaxed),
            _ => {}
        }
    }
}
impl AsyncRead for Observed {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            self.scanner.feed(&buf.filled()[before..]);
        }
        result
    }
}
impl AsyncWrite for Observed {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write(cx, buf);
        self.written(&result, buf.len());
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let result = Pin::new(&mut self.io).poll_flush(cx);
        match &result {
            Poll::Ready(Ok(())) => {
                self.scanner
                    .shared
                    .write_pending
                    .store(false, Ordering::Relaxed);
                if self.dirty {
                    self.dirty = false;
                    self.scanner.shared.output_ms.store(
                        self.scanner
                            .shared
                            .origin
                            .elapsed()
                            .as_millis()
                            .min(u64::MAX as u128) as u64,
                        Ordering::Relaxed,
                    );
                }
            }
            Poll::Pending => self
                .scanner
                .shared
                .write_pending
                .store(true, Ordering::Relaxed),
            _ => {}
        }
        result
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let result = Pin::new(&mut self.io).poll_write_vectored(cx, buf);
        self.written(&result, buf.iter().map(|v| v.len()).sum());
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn setup() -> (Scanner, HeaderClock) {
        let shared = Arc::new(Shared::default());
        (
            Scanner {
                shared: shared.clone(),
                state: State::Head,
                buffer: Vec::new(),
                trailers: 0,
            },
            HeaderClock {
                shared,
                header: Some(Duration::from_millis(20)),
                idle: Duration::from_millis(100),
            },
        )
    }
    #[tokio::test(start_paused = true)]
    async fn idle_then_partial_and_already_buffered_prefix_use_distinct_budgets() {
        let (mut scan, clock) = setup();
        drop(clock.head_sleep());
        scan.feed(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        let mut idle = Box::pin(clock.head_sleep());
        tokio::time::advance(Duration::from_millis(70)).await;
        assert!(futures_util::poll!(idle.as_mut()).is_pending());
        scan.feed(b"GET /partial");
        assert!(futures_util::poll!(idle.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(21)).await;
        assert!(futures_util::poll!(idle.as_mut()).is_ready());
        let (mut scan, clock) = setup();
        scan.feed(b"GET / HTTP/1.1\r\n\r\nGET /partial");
        drop(clock.head_sleep());
        let mut next = Box::pin(clock.head_sleep());
        tokio::time::advance(Duration::from_millis(21)).await;
        assert!(futures_util::poll!(next.as_mut()).is_ready());
    }
    #[tokio::test(start_paused = true)]
    async fn output_backpressure_delays_idle_and_expiry_is_a_quiet_signal() {
        let (mut scan, clock) = setup();
        drop(clock.head_sleep());
        scan.feed(b"GET / HTTP/1.1\r\n\r\n");
        let mut idle = Box::pin(clock.head_sleep());
        clock.shared.write_pending.store(true, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(150)).await;
        assert!(futures_util::poll!(idle.as_mut()).is_pending());
        assert!(!clock.shared.idle_expired.load(Ordering::Relaxed));
        clock.shared.write_pending.store(false, Ordering::Relaxed);
        clock.shared.output_ms.store(150, Ordering::Relaxed);
        assert!(futures_util::poll!(idle.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(101)).await;
        assert!(futures_util::poll!(idle.as_mut()).is_pending());
        assert!(clock.shared.idle_expired.load(Ordering::Relaxed));
        clock.expired().await;
    }
    #[tokio::test]
    async fn framing_observer_handles_fragmented_fixed_chunked_and_trailers_with_a_bound() {
        let bytes=b"POST /a HTTP/1.1\r\nContent-Length: 3\r\n\r\nabcPOST /b HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3;ext=x\r\nabc\r\n0\r\nX-End: yes\r\n\r\nGET /partial";
        for chunk in [1, 2, 7, 19, 4096] {
            let (mut scanner, clock) = setup();
            for part in bytes.chunks(chunk) {
                scanner.feed(part);
            }
            assert_eq!(clock.shared.frontier.load(Ordering::Relaxed), 3);
            assert!(!clock.shared.uncertain.load(Ordering::Relaxed));
        }
        let (mut scanner, clock) = setup();
        scanner.feed(&vec![b'x'; 40000]);
        assert!(clock.shared.uncertain.load(Ordering::Relaxed));
        assert!(scanner.buffer.capacity() <= 32768);
    }
}
