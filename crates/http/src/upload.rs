use crate::{Error, IncomingBody};
use bytes::Bytes;
use hyper::body::{Body, Frame, SizeHint};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    sync::Notify,
    time::{Instant, Sleep},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HttpFailure {
    BodyTooLarge,
    BodyTimeout,
    InvalidBody,
    ResponseHeaderTimeout,
}
impl HttpFailure {
    pub fn status(self) -> u16 {
        match self {
            Self::BodyTooLarge => 413,
            Self::BodyTimeout => 408,
            Self::InvalidBody => 400,
            Self::ResponseHeaderTimeout => 504,
        }
    }
    pub fn is_client(self) -> bool {
        self != Self::ResponseHeaderTimeout
    }
    pub fn message(self) -> &'static str {
        match self {
            Self::BodyTooLarge => "request body too large\n",
            Self::BodyTimeout => "request body idle timeout\n",
            Self::InvalidBody => "invalid request body\n",
            Self::ResponseHeaderTimeout => "upstream response header timeout\n",
        }
    }
}
impl std::fmt::Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message().trim_end())
    }
}
impl std::error::Error for HttpFailure {}

#[derive(Clone, Copy, Default)]
pub struct RequestPolicy {
    pub max_bytes: Option<u64>,
    pub idle_timeout_ms: Option<u64>,
}
impl RequestPolicy {
    /// Use the decoder's size hint rather than a header that a filter can edit.
    pub fn check<B: Body>(&self, body: &B) -> Result<(), HttpFailure> {
        if self
            .max_bytes
            .filter(|n| *n > 0)
            .is_some_and(|max| body.size_hint().exact().is_some_and(|n| n > max))
        {
            Err(HttpFailure::BodyTooLarge)
        } else {
            Ok(())
        }
    }
}

pub(crate) struct UploadState {
    failure: AtomicU8,
    complete: AtomicBool,
    ended: Mutex<Option<Instant>>,
    pub changed: Notify,
    response_waker: Mutex<Option<Waker>>,
}
impl UploadState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            failure: AtomicU8::new(0),
            complete: AtomicBool::new(false),
            ended: Mutex::new(None),
            changed: Notify::new(),
            response_waker: Mutex::new(None),
        })
    }
    pub fn failure(&self) -> Option<HttpFailure> {
        match self.failure.load(Ordering::Acquire) {
            1 => Some(HttpFailure::BodyTooLarge),
            2 => Some(HttpFailure::BodyTimeout),
            3 => Some(HttpFailure::InvalidBody),
            _ => None,
        }
    }
    fn fail(&self, error: HttpFailure) {
        self.failure.store(
            match error {
                HttpFailure::BodyTooLarge => 1,
                HttpFailure::BodyTimeout => 2,
                _ => 3,
            },
            Ordering::Release,
        );
        self.changed.notify_one();
        let waker = self.response_waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    pub fn complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    pub fn ended(&self) -> Option<Instant> {
        *self.ended.lock().unwrap()
    }
    fn finish(&self) {
        if !self.complete() {
            *self.ended.lock().unwrap() = Some(Instant::now());
            self.complete.store(true, Ordering::Release);
            self.changed.notify_one();
        }
    }
}

/// Clear the task waker when a response is dropped, including cancellation.
pub(crate) struct UploadWatch(pub Arc<UploadState>);
impl UploadWatch {
    pub fn poll_failure(&self, cx: &Context<'_>) -> Option<HttpFailure> {
        let mut waker = self.0.response_waker.lock().unwrap();
        if waker.as_ref().is_none_or(|old| !old.will_wake(cx.waker())) {
            *waker = Some(cx.waker().clone());
        }
        self.0.failure()
    }
}
impl Drop for UploadWatch {
    fn drop(&mut self) {
        self.0.response_waker.lock().unwrap().take();
    }
}

pub enum RequestBody {
    Empty,
    Plain(IncomingBody),
    Guarded(Box<UploadBody<IncomingBody>>),
}
impl RequestBody {
    pub(crate) fn new(
        inner: IncomingBody,
        policy: RequestPolicy,
        track_eof: bool,
    ) -> (Self, Option<Arc<UploadState>>) {
        let limit = policy.max_bytes.filter(|n| *n > 0);
        let idle = policy
            .idle_timeout_ms
            .filter(|n| *n > 0)
            .map(Duration::from_millis);
        if inner.is_end_stream() || (limit.is_none() && idle.is_none() && !track_eof) {
            return (Self::Plain(inner), None);
        }
        let state = UploadState::new();
        (
            Self::Guarded(Box::new(UploadBody {
                inner,
                state: state.clone(),
                remaining: limit,
                timeout: idle,
                timer: None,
                armed: false,
                finished: false,
            })),
            Some(state),
        )
    }
}
impl Body for RequestBody {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        match &mut *self {
            Self::Empty => Poll::Ready(None),
            Self::Plain(inner) => Pin::new(inner)
                .poll_frame(cx)
                .map(|f| f.map(|r| r.map_err(|_| Box::new(HttpFailure::InvalidBody) as Error))),
            Self::Guarded(inner) => Pin::new(&mut **inner).poll_frame(cx),
        }
    }
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Plain(i) => i.is_end_stream(),
            Self::Guarded(i) => i.is_end_stream(),
        }
    }
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Empty => SizeHint::with_exact(0),
            Self::Plain(i) => i.size_hint(),
            Self::Guarded(i) => i.size_hint(),
        }
    }
}

// Generic to test framing, backpressure, trailers and timeouts without a socket.
pub struct UploadBody<B> {
    inner: B,
    state: Arc<UploadState>,
    remaining: Option<u64>,
    timeout: Option<Duration>,
    timer: Option<Pin<Box<Sleep>>>,
    armed: bool,
    finished: bool,
}
impl<B: Body<Data = Bytes> + Unpin> Body for UploadBody<B>
where
    B::Error: Into<Error>,
{
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let expired = self.armed
            && self
                .timer
                .as_mut()
                .is_some_and(|t| t.as_mut().poll(cx).is_ready());
        let frame = if expired {
            Err(HttpFailure::BodyTimeout)
        } else {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Pending => {
                    if let Some(timeout) = self.timeout {
                        if !self.armed {
                            let deadline = Instant::now() + timeout;
                            if let Some(t) = &mut self.timer {
                                t.as_mut().reset(deadline);
                            } else {
                                self.timer = Some(Box::pin(tokio::time::sleep_until(deadline)));
                            }
                            self.armed = true;
                        }
                        // Register this task even on the first pending read.
                        let _ = self.timer.as_mut().unwrap().as_mut().poll(cx);
                    }
                    return Poll::Pending;
                }
                Poll::Ready(None) => {
                    self.state.finish();
                    self.finished = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(_))) => Err(HttpFailure::InvalidBody),
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        if let Some(remaining) = &mut self.remaining {
                            if data.len() as u64 > *remaining {
                                self.state.fail(HttpFailure::BodyTooLarge);
                                self.finished = true;
                                return Poll::Ready(Some(Err(HttpFailure::BodyTooLarge.into())));
                            }
                            *remaining -= data.len() as u64;
                        }
                        if !data.is_empty() {
                            self.armed = false;
                        }
                    }
                    if frame.is_trailers() || self.inner.is_end_stream() {
                        self.state.finish();
                        self.finished = true;
                    }
                    Ok(frame)
                }
            }
        };
        Poll::Ready(Some(match frame {
            Ok(frame) => Ok(frame),
            Err(error) => {
                self.state.fail(error);
                self.finished = true;
                Err(error.into())
            }
        }))
    }
    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    struct Channel(tokio::sync::mpsc::UnboundedReceiver<Result<Frame<Bytes>, std::io::Error>>);
    impl Body for Channel {
        type Data = Bytes;
        type Error = std::io::Error;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            self.0.poll_recv(cx)
        }
    }
    fn body(
        limit: u64,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<Result<Frame<Bytes>, std::io::Error>>,
        UploadBody<Channel>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            tx,
            UploadBody {
                inner: Channel(rx),
                state: UploadState::new(),
                remaining: Some(limit),
                timeout: Some(Duration::from_millis(50)),
                timer: None,
                armed: false,
                finished: false,
            },
        )
    }
    #[tokio::test]
    async fn exact_limit_preserves_trailers_and_rejects_entire_oversize_frame() {
        let (tx, mut body) = body(3);
        tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
            .unwrap();
        let mut trailers = http::HeaderMap::new();
        trailers.insert("x-end", http::HeaderValue::from_static("yes"));
        tx.send(Ok(Frame::trailers(trailers))).unwrap();
        assert_eq!(
            body.frame().await.unwrap().unwrap().into_data().unwrap(),
            "abc"
        );
        assert_eq!(
            body.frame()
                .await
                .unwrap()
                .unwrap()
                .into_trailers()
                .unwrap()["x-end"],
            "yes"
        );
        assert!(body.state.complete());
        assert!(body.frame().await.is_none());
        let (tx, mut body) = self::body(2);
        tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
            .unwrap();
        assert_eq!(
            *body
                .frame()
                .await
                .unwrap()
                .unwrap_err()
                .downcast::<HttpFailure>()
                .unwrap(),
            HttpFailure::BodyTooLarge
        );
        assert_eq!(body.state.failure(), Some(HttpFailure::BodyTooLarge));
        assert!(body.frame().await.is_none());
    }
    #[tokio::test(start_paused = true)]
    async fn pending_upload_times_out_and_wakes_early_response() {
        let (_tx, mut body) = body(100);
        let mut response =
            Box::pin(crate::Body::text("ok").with_upload(UploadWatch(body.state.clone())));
        assert_eq!(
            *body
                .frame()
                .await
                .unwrap()
                .unwrap_err()
                .downcast::<HttpFailure>()
                .unwrap(),
            HttpFailure::BodyTimeout
        );
        assert_eq!(
            *response
                .frame()
                .await
                .unwrap()
                .unwrap_err()
                .downcast::<HttpFailure>()
                .unwrap(),
            HttpFailure::BodyTimeout
        );
    }
    #[tokio::test(start_paused = true)]
    async fn empty_frames_do_not_refresh_upload_deadline() {
        let (tx, mut body) = body(100);
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut body).poll_frame(cx)))
                .await
                .is_pending()
        );
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(Ok(Frame::data(Bytes::new()))).unwrap();
        body.frame().await.unwrap().unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(Ok(Frame::data(Bytes::from_static(b"late"))))
            .unwrap();
        assert_eq!(body.state.failure(), None);
        assert_eq!(
            *body
                .frame()
                .await
                .unwrap()
                .unwrap_err()
                .downcast::<HttpFailure>()
                .unwrap(),
            HttpFailure::BodyTimeout
        );
    }
}
