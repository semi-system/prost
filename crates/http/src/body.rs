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
#[derive(Clone)]
pub struct KeepAlive {
    _generation: Arc<dyn Send + Sync>,
    _connection: Arc<dyn Send + Sync>,
}
impl KeepAlive {
    pub(crate) fn with_activity(mut self, activity: Arc<crate::pool_stats::Activity>) -> Self {
        self._generation = Arc::new((self._generation, activity));
        self
    }
    pub(crate) fn with_admission(mut self, permit: OwnedSemaphorePermit) -> Self {
        self._generation = Arc::new((self._generation, permit));
        self
    }
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
        Custom { inner: Pin<Box<dyn HttpBody<Data=Bytes, Error=Error> + Send>>, guard: Option<Guard> },
        Static { #[pin] inner: Full<Bytes> },
        Incoming { #[pin] inner: Incoming },
        Buffered { #[pin] inner: Full<Bytes>, guard: Guard },
        Observed { inner: crate::observe::Observed },
        Upload { inner: Option<Pin<Box<Body>>>, watch: crate::upload::UploadWatch, guard: Option<Guard> },
        Trailers { inner: Pin<Box<Body>>, trailers: Option<http::HeaderMap>, finished: bool, guard: Option<Guard> },
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
    pub(crate) fn custom<B: HttpBody<Data = Bytes, Error = Error> + Send + 'static>(
        mut base: Body,
        wrap: impl FnOnce(Body) -> B,
    ) -> Self {
        let guard = base.take_guard();
        Self {
            inner: Kind::Custom {
                inner: Box::pin(wrap(base)),
                guard,
            },
        }
    }
    /// Optional wrapper; disabled logging does not allocate an observer/body box.
    pub fn observe(self, observer: Arc<dyn crate::BodyObserver>) -> Self {
        Self {
            inner: Kind::Observed {
                inner: crate::observe::Observed::new(self, observer),
            },
        }
    }
    /// Optional wrapper allocated only when a native trailer rule is active.
    pub fn with_trailers(mut self, trailers: http::HeaderMap) -> Self {
        let guard = self.take_guard();
        Self {
            inner: Kind::Trailers {
                inner: Box::pin(self),
                trailers: Some(trailers),
                finished: false,
                guard,
            },
        }
    }
    fn take_guard(&mut self) -> Option<Guard> {
        match &mut self.inner {
            Kind::Buffered { guard, .. } | Kind::Stream { guard, .. } => Some(std::mem::replace(
                guard,
                Guard {
                    permit: None,
                    keepalive: None,
                    completion: Completion(None),
                },
            )),
            Kind::Upload { guard, .. }
            | Kind::Trailers { guard, .. }
            | Kind::Custom { guard, .. } => guard.take(),
            _ => None,
        }
    }
    pub(crate) fn with_upload(mut self, watch: crate::upload::UploadWatch) -> Self {
        let guard = self.take_guard();
        Self {
            inner: Kind::Upload {
                inner: Some(Box::pin(self)),
                watch,
                guard,
            },
        }
    }
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
            KindProj::Custom { inner, guard } => {
                let result = inner.as_mut().poll_frame(cx);
                if matches!(&result, Poll::Ready(None) | Poll::Ready(Some(Err(_))))
                    || inner.is_end_stream()
                {
                    if let Some(mut guard) = guard.take() {
                        guard.finish();
                    }
                }
                result
            }
            KindProj::Observed { inner } => Pin::new(inner).poll_frame(cx),
            KindProj::Upload {
                inner,
                watch,
                guard,
            } => {
                let Some(body) = inner else {
                    return Poll::Ready(None);
                };
                if let Some(error) = watch.poll_failure(cx) {
                    inner.take();
                    if let Some(guard) = guard {
                        guard.finish();
                    }
                    return Poll::Ready(Some(Err(error.into())));
                }
                let frame = body.as_mut().poll_frame(cx);
                if matches!(&frame, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
                    inner.take();
                    if let Some(guard) = guard {
                        guard.finish();
                    }
                }
                frame
            }
            KindProj::Trailers {
                inner,
                trailers,
                finished,
                guard,
            } => {
                if *finished {
                    return Poll::Ready(None);
                }
                match inner.as_mut().poll_frame(cx) {
                    Poll::Ready(Some(Ok(frame))) => match frame.into_trailers() {
                        Ok(mut original) => {
                            if let Some(extra) = trailers.take() {
                                for (name, value) in extra.iter() {
                                    original.append(name.clone(), value.clone());
                                }
                            }
                            *finished = true;
                            if let Some(g) = guard {
                                g.finish();
                            }
                            Poll::Ready(Some(Ok(Frame::trailers(original))))
                        }
                        Err(data) => Poll::Ready(Some(Ok(data))),
                    },
                    Poll::Ready(None) => {
                        *finished = true;
                        if let Some(g) = guard {
                            g.finish();
                        }
                        Poll::Ready(trailers.take().map(|h| Ok(Frame::trailers(h))))
                    }
                    Poll::Ready(Some(Err(e))) => {
                        *finished = true;
                        if let Some(g) = guard {
                            g.finish();
                        }
                        Poll::Ready(Some(Err(e)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
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
                        if timeout.is_zero() {
                            return Poll::Pending;
                        }
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
            Kind::Custom { inner, .. } => inner.is_end_stream(),
            Kind::Observed { inner } => inner.is_end_stream(),
            Kind::Upload { inner, watch, .. } => {
                inner.is_none()
                    || (watch.0.failure().is_none()
                        && inner.as_ref().is_none_or(|b| b.is_end_stream()))
            }
            Kind::Trailers { finished, .. } => *finished,
            Kind::Static { inner } | Kind::Buffered { inner, .. } => inner.is_end_stream(),
            Kind::Incoming { inner } => inner.is_end_stream(),
            Kind::Stream { inner, prefix, .. } => prefix.is_none() && inner.is_end_stream(),
        }
    }
    fn size_hint(&self) -> SizeHint {
        match &self.inner {
            Kind::Custom { inner, .. } => inner.size_hint(),
            Kind::Observed { inner } => inner.size_hint(),
            Kind::Upload { inner, .. } => inner
                .as_ref()
                .map_or_else(SizeHint::default, |b| b.size_hint()),
            Kind::Trailers { .. } => SizeHint::default(),
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
