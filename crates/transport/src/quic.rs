//! Quinn adapter. Observe STOP_SENDING even while the HTTP handler
//! is waiting on an idle origin; cancellation must not wait for another write.
use bytes::{Buf, Bytes};
use h3::quic::{
    self, ConnectionErrorIncoming as ConnError, StreamErrorIncoming as StreamError, StreamId,
    WriteBuf,
};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

pub type Cancellations = Arc<CancellationSet>;
pub struct CancellationSet {
    streams: Mutex<HashMap<u64, CancellationToken>>,
    limit: usize,
    capacity: futures_util::task::AtomicWaker,
    drained: futures_util::task::AtomicWaker,
}
impl Default for CancellationSet {
    fn default() -> Self {
        Self::new(1024)
    }
}
impl CancellationSet {
    pub fn new(limit: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            limit,
            capacity: futures_util::task::AtomicWaker::new(),
            drained: futures_util::task::AtomicWaker::new(),
        }
    }
    pub fn get(&self, id: u64) -> Option<CancellationToken> {
        self.streams.lock().unwrap().get(&id).cloned()
    }
    fn remove(&self, id: u64) {
        self.streams.lock().unwrap().remove(&id);
        self.capacity.wake();
        self.drained.wake();
    }
    pub async fn wait_delivered(&self) {
        std::future::poll_fn(|cx| {
            self.drained.register(cx.waker());
            if self.streams.lock().unwrap().is_empty() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
type Accept = Pin<
    Box<
        dyn Future<Output = Result<(quinn::SendStream, quinn::RecvStream), quinn::ConnectionError>>
            + std::marker::Send,
    >,
>;
type AcceptUni = Pin<
    Box<dyn Future<Output = Result<quinn::RecvStream, quinn::ConnectionError>> + std::marker::Send>,
>;
type OpenUni = Pin<
    Box<dyn Future<Output = Result<quinn::SendStream, quinn::ConnectionError>> + std::marker::Send>,
>;
pub struct Connection {
    socket: quinn::Connection,
    accepting: Option<Accept>,
    accepting_uni: Option<AcceptUni>,
    opening: Opener,
    signals: Cancellations,
}
impl Connection {
    pub fn new(socket: quinn::Connection, signals: Cancellations) -> Self {
        Self::with_role(socket, signals, false)
    }
    pub fn client(socket: quinn::Connection) -> Self {
        Self::with_role(socket, Arc::default(), true)
    }
    fn with_role(socket: quinn::Connection, signals: Cancellations, client: bool) -> Self {
        Self {
            opening: Opener::new(socket.clone(), client),
            socket,
            accepting: None,
            accepting_uni: None,
            signals,
        }
    }
}
pub struct Opener {
    socket: quinn::Connection,
    client: bool,
    bidi: Option<Accept>,
    uni: Option<OpenUni>,
}
impl Opener {
    fn new(socket: quinn::Connection, client: bool) -> Self {
        Self {
            socket,
            client,
            bidi: None,
            uni: None,
        }
    }
}
fn no_server_bidi<T>() -> Poll<Result<T, StreamError>> {
    Poll::Ready(Err(StreamError::ConnectionErrorIncoming {
        connection_error: ConnError::InternalError(
            "HTTP/3 servers cannot initiate bidirectional request streams".into(),
        ),
    }))
}
impl quic::OpenStreams<Bytes> for Opener {
    type BidiStream = Bidi;
    type SendStream = Send;
    fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Bidi, StreamError>> {
        if !self.client {
            return no_server_bidi();
        }
        let socket = self.socket.clone();
        let future = self
            .bidi
            .get_or_insert_with(|| Box::pin(async move { socket.open_bi().await }));
        let result = match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.bidi = None;
        Poll::Ready(
            result
                .map(|(send, recv)| bidi(send, recv, None))
                .map_err(|e| StreamError::ConnectionErrorIncoming {
                    connection_error: connection_error(e),
                }),
        )
    }
    fn poll_open_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<Send, StreamError>> {
        let socket = self.socket.clone();
        let future = self
            .uni
            .get_or_insert_with(|| Box::pin(async move { socket.open_uni().await }));
        let result = match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.uni = None;
        Poll::Ready(result.map(|stream| Send::new(stream, None)).map_err(|e| {
            StreamError::ConnectionErrorIncoming {
                connection_error: connection_error(e),
            }
        }))
    }
    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        self.socket.close(
            quinn::VarInt::from_u64(u64::from(code)).unwrap_or(quinn::VarInt::MAX),
            reason,
        )
    }
}
impl quic::OpenStreams<Bytes> for Connection {
    type BidiStream = Bidi;
    type SendStream = Send;
    fn poll_open_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Bidi, StreamError>> {
        self.opening.poll_open_bidi(cx)
    }
    fn poll_open_send(&mut self, cx: &mut Context<'_>) -> Poll<Result<Send, StreamError>> {
        self.opening.poll_open_send(cx)
    }
    fn close(&mut self, code: h3::error::Code, reason: &[u8]) {
        self.opening.close(code, reason)
    }
}
impl quic::Connection<Bytes> for Connection {
    type RecvStream = Recv;
    type OpenStreams = Opener;
    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, ConnError>> {
        let socket = self.socket.clone();
        let future = self
            .accepting_uni
            .get_or_insert_with(|| Box::pin(async move { socket.accept_uni().await }));
        let result = match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.accepting_uni = None;
        Poll::Ready(result.map(Recv).map_err(connection_error))
    }
    fn poll_accept_bidi(&mut self, cx: &mut Context<'_>) -> Poll<Result<Bidi, ConnError>> {
        self.signals.capacity.register(cx.waker());
        if self.signals.streams.lock().unwrap().len() >= self.signals.limit {
            return Poll::Pending;
        }
        let socket = self.socket.clone();
        let future = self
            .accepting
            .get_or_insert_with(|| Box::pin(async move { socket.accept_bi().await }));
        let result = match future.as_mut().poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(result) => result,
        };
        self.accepting = None;
        Poll::Ready(result.map_err(connection_error).map(|(send, recv)| {
            bidi(
                send,
                recv,
                (!self.opening.client).then(|| self.signals.clone()),
            )
        }))
    }
    fn opener(&self) -> Opener {
        Opener::new(self.socket.clone(), self.opening.client)
    }
}
fn bidi(send: quinn::SendStream, recv: quinn::RecvStream, signals: Option<Cancellations>) -> Bidi {
    Bidi {
        send: Send::new(send, signals),
        recv: Recv(recv),
    }
}

pub struct Bidi {
    send: Send,
    recv: Recv,
}
impl quic::BidiStream<Bytes> for Bidi {
    type SendStream = Send;
    type RecvStream = Recv;
    fn split(self) -> (Send, Recv) {
        (self.send, self.recv)
    }
}
impl quic::RecvStream for Bidi {
    type Buf = Bytes;
    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, StreamError>> {
        self.recv.poll_data(cx)
    }
    fn stop_sending(&mut self, code: u64) {
        self.recv.stop_sending(code)
    }
    fn recv_id(&self) -> StreamId {
        self.recv.recv_id()
    }
}
impl quic::SendStream<Bytes> for Bidi {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        self.send.poll_ready(cx)
    }
    fn send_data<D: Into<WriteBuf<Bytes>>>(&mut self, data: D) -> Result<(), StreamError> {
        self.send.send_data(data)
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        self.send.poll_finish(cx)
    }
    fn reset(&mut self, code: u64) {
        self.send.reset(code)
    }
    fn send_id(&self) -> StreamId {
        self.send.send_id()
    }
}
pub struct Recv(quinn::RecvStream);
impl Drop for Recv {
    fn drop(&mut self) {
        let _ = self.0.stop(quinn::VarInt::from_u32(0x10c));
    }
}
impl quic::RecvStream for Recv {
    type Buf = Bytes;
    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Bytes>, StreamError>> {
        // read_chunk is cancel-safe and stores readiness in Quinn itself. Keep
        // ownership here, so STOP_SENDING remains safe after a pending read.
        let reading = self.0.read_chunk(16384, true);
        tokio::pin!(reading);
        reading.poll(cx).map(|result| {
            result
                .map(|chunk| chunk.map(|chunk| chunk.bytes))
                .map_err(read_error)
        })
    }
    fn stop_sending(&mut self, code: u64) {
        let _ = self
            .0
            .stop(quinn::VarInt::from_u64(code).unwrap_or(quinn::VarInt::MAX));
    }
    fn recv_id(&self) -> StreamId {
        u64::from(self.0.id())
            .try_into()
            .expect("QUIC stream IDs fit HTTP/3")
    }
}
pub struct Send {
    stream: quinn::SendStream,
    pending: Option<WriteBuf<Bytes>>,
    watch: Option<tokio::task::JoinHandle<()>>,
    signals: Option<Cancellations>,
    id: u64,
    finished: bool,
    reset: bool,
}
impl Send {
    fn new(stream: quinn::SendStream, signals: Option<Cancellations>) -> Self {
        let id = u64::from(stream.id());
        let watch = signals.as_ref().map(|signals| {
            let token = CancellationToken::new();
            signals.streams.lock().unwrap().insert(id, token.clone());
            let signals = signals.clone();
            let stopped = stream.stopped();
            tokio::spawn(async move {
                if !matches!(stopped.await, Ok(None)) {
                    token.cancel();
                }
                signals.remove(id);
            })
        });
        Self {
            stream,
            pending: None,
            watch,
            signals,
            id,
            finished: false,
            reset: false,
        }
    }
}
impl Drop for Send {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.stream.reset(quinn::VarInt::from_u32(0x10c));
        }
        if !self.finished || self.reset {
            if let Some(watch) = &self.watch {
                watch.abort();
            }
            if let Some(signals) = &self.signals {
                signals.remove(self.id);
            }
        }
        // A successful FIN keeps its bounded delivery watcher until ACK. It
        // owns no application body; draining waits for these before closing.
    }
}
impl quic::SendStream<Bytes> for Send {
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        if let Some(data) = &mut self.pending {
            while data.has_remaining() {
                let count = match Pin::new(&mut self.stream).poll_write(cx, data.chunk()) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(n)) => n,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(write_error(error))),
                };
                if count == 0 {
                    return Poll::Ready(Err(StreamError::Unknown(Box::new(std::io::Error::from(
                        std::io::ErrorKind::WriteZero,
                    )))));
                }
                data.advance(count);
            }
        }
        self.pending = None;
        Poll::Ready(Ok(()))
    }
    fn send_data<D: Into<WriteBuf<Bytes>>>(&mut self, data: D) -> Result<(), StreamError> {
        if self.pending.is_some() {
            return Err(StreamError::Unknown(Box::new(std::io::Error::from(
                std::io::ErrorKind::WouldBlock,
            ))));
        }
        self.pending = Some(data.into());
        Ok(())
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        match self.poll_ready(cx) {
            Poll::Ready(Ok(())) => {
                let result = self
                    .stream
                    .finish()
                    .map_err(|error| StreamError::Unknown(Box::new(error)));
                if result.is_ok() {
                    self.finished = true;
                }
                Poll::Ready(result)
            }
            other => other,
        }
    }
    fn reset(&mut self, code: u64) {
        self.finished = true;
        self.reset = true;
        let _ = self
            .stream
            .reset(quinn::VarInt::from_u64(code).unwrap_or(quinn::VarInt::MAX));
    }
    fn send_id(&self) -> StreamId {
        self.id.try_into().expect("QUIC stream IDs fit HTTP/3")
    }
}
fn connection_error(error: quinn::ConnectionError) -> ConnError {
    match error {
        quinn::ConnectionError::ApplicationClosed(error) => ConnError::ApplicationClose {
            error_code: error.error_code.into_inner(),
        },
        quinn::ConnectionError::TimedOut => ConnError::Timeout,
        error => ConnError::Undefined(Arc::new(error)),
    }
}
fn read_error(error: quinn::ReadError) -> StreamError {
    match error {
        quinn::ReadError::Reset(code) => StreamError::StreamTerminated {
            error_code: code.into_inner(),
        },
        quinn::ReadError::ConnectionLost(error) => StreamError::ConnectionErrorIncoming {
            connection_error: connection_error(error),
        },
        error => StreamError::Unknown(Box::new(error)),
    }
}
fn write_error(error: quinn::WriteError) -> StreamError {
    match error {
        quinn::WriteError::Stopped(code) => StreamError::StreamTerminated {
            error_code: code.into_inner(),
        },
        quinn::WriteError::ConnectionLost(error) => StreamError::ConnectionErrorIncoming {
            connection_error: connection_error(error),
        },
        error => StreamError::Unknown(Box::new(error)),
    }
}
