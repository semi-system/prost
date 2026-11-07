use crate::Error;
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::{Body as HttpBody, Frame, Incoming, SizeHint};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{oneshot, OwnedSemaphorePermit},
    time::Sleep,
};

pub(crate) struct Completion(pub Option<oneshot::Sender<()>>);
impl Completion {
    fn finish(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finish();
    }
}

pub(crate) struct Guard {
    pub permit: Option<OwnedSemaphorePermit>,
    pub keepalive: Option<KeepAlive>,
    pub completion: Completion,
}
/// Keep both the config generation and accepted-connection admission alive.
pub struct KeepAlive {
    _generation: Arc<dyn Send + Sync>,
    _connection: Arc<dyn Send + Sync>,
}
impl KeepAlive {
    pub fn new<T: Send + Sync + 'static, U: Send + Sync + 'static>(
        generation: Arc<T>,
        connection: Arc<U>,
    ) -> Self {
        Self {
            _generation: generation,
            _connection: connection,
        }
    }
}
impl Guard {
    fn finish(&mut self) {
        self.completion.finish();
        self.permit.take();
        self.keepalive.take();
    }
}

pin_project! {
    /// Concrete body: no trait-object allocation, with an inline, lazy idle timer.
    pub struct Body { #[pin] inner: Kind }
}
pin_project! {
    #[project = KindProj]
    enum Kind {
        Static { #[pin] inner: Full<Bytes> },
        Incoming { #[pin] inner: Incoming },
        Buffered { #[pin] inner: Full<Bytes>, guard: Guard },
        Stream {
            #[pin] inner: Incoming,
            #[pin] idle: Option<Sleep>,
            timeout: Duration,
            failed: bool,
            guard: Guard,
            prefix: Option<Frame<Bytes>>,
        },
    }
}
impl Body {
    pub(crate) fn text(text: &'static str) -> Self {
        Self {
            inner: Kind::Static {
                inner: Full::new(Bytes::from_static(text.as_bytes())),
            },
        }
    }
    pub(crate) fn incoming(inner: Incoming) -> Self {
        Self {
            inner: Kind::Incoming { inner },
        }
    }
    pub(crate) fn buffered(data: Bytes, guard: Guard) -> Self {
        Self {
            inner: Kind::Buffered {
                inner: Full::new(data),
                guard,
            },
        }
    }
    pub(crate) fn stream(
        inner: Incoming,
        prefix: Option<Frame<Bytes>>,
        timeout: Duration,
        guard: Guard,
    ) -> Self {
        Self {
            inner: Kind::Stream {
                inner,
                prefix,
                idle: None,
                timeout,
                failed: false,
                guard,
            },
        }
    }
}
impl HttpBody for Body {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        match self.project().inner.project() {
            KindProj::Static { inner } => inner
                .poll_frame(cx)
                .map(|frame| frame.map(|r| r.map_err(|e| match e {}))),
            KindProj::Incoming { inner } => inner
                .poll_frame(cx)
                .map(|frame| frame.map(|r| r.map_err(|e| Box::new(e) as Error))),
            KindProj::Buffered { inner, guard } => {
                let frame = inner.poll_frame(cx);
                if matches!(frame, Poll::Ready(None)) {
                    guard.finish();
                }
                frame.map(|frame| frame.map(|r| r.map_err(|e| match e {})))
            }
            KindProj::Stream {
                mut inner,
                mut idle,
                timeout,
                failed,
                guard,
                prefix,
            } => {
                if *failed {
                    return Poll::Ready(None);
                }
                if let Some(frame) = prefix.take() {
                    return Poll::Ready(Some(Ok(frame)));
                }
                match inner.as_mut().poll_frame(cx) {
                    Poll::Ready(frame) => {
                        idle.set(None);
                        if frame.is_none() || matches!(&frame, Some(Err(_))) {
                            guard.finish();
                        }
                        Poll::Ready(frame.map(|r| r.map_err(|e| Box::new(e) as Error)))
                    }
                    Poll::Pending => {
                        if idle.is_none() {
                            idle.as_mut().set(Some(tokio::time::sleep(*timeout)));
                        }
                        if idle.as_mut().as_pin_mut().unwrap().poll(cx).is_ready() {
                            *failed = true;
                            guard.finish();
                            Poll::Ready(Some(Err(std::io::Error::new(
                                std::io::ErrorKind::TimedOut,
                                "body idle timeout",
                            )
                            .into())))
                        } else {
                            Poll::Pending
                        }
                    }
                }
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        match &self.inner {
            Kind::Static { inner } | Kind::Buffered { inner, .. } => inner.is_end_stream(),
            Kind::Incoming { inner } => inner.is_end_stream(),
            Kind::Stream { inner, prefix, .. } => prefix.is_none() && inner.is_end_stream(),
        }
    }
    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Kind::Static { inner } | Kind::Buffered { inner, .. } => inner.size_hint(),
            Kind::Incoming { inner } => inner.size_hint(),
            Kind::Stream { inner, prefix, .. } => {
                let hint = inner.size_hint();
                let count = prefix
                    .as_ref()
                    .and_then(|f| f.data_ref())
                    .map_or(0, |bytes| bytes.len() as u64);
                let mut total = SizeHint::new();
                total.set_lower(hint.lower().saturating_add(count));
                if let Some(max) = hint.upper().and_then(|n| n.checked_add(count)) {
                    total.set_upper(max);
                }
                total
            }
        }
    }
}
