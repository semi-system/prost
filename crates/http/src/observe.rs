use crate::{Body, Error};
use bytes::Bytes;
use hyper::body::{Body as HttpBody, Frame, SizeHint};
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyEnd {
    Complete,
    Error,
    Aborted,
}
/// Observes DATA supplied to the downstream HTTP driver, not encrypted wire bytes.
pub trait BodyObserver: Send + Sync {
    fn end(&self, bytes: u64, result: BodyEnd);
}
pub(crate) struct Observed {
    inner: Option<Pin<Box<Body>>>,
    observer: Arc<dyn BodyObserver>,
    bytes: u64,
    finished: bool,
}
impl Observed {
    pub fn new(inner: Body, observer: Arc<dyn BodyObserver>) -> Self {
        Self {
            inner: Some(Box::pin(inner)),
            observer,
            bytes: 0,
            finished: false,
        }
    }
    fn finish(&mut self, result: BodyEnd) {
        if !self.finished {
            self.finished = true;
            self.observer.end(self.bytes, result);
        }
    }
}
impl Drop for Observed {
    fn drop(&mut self) {
        self.finish(BodyEnd::Aborted);
    }
}
impl HttpBody for Observed {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let frame = self.inner.as_mut().unwrap().as_mut().poll_frame(cx);
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    self.bytes = self.bytes.saturating_add(data.len() as u64);
                }
                if frame.is_trailers() || self.inner.as_ref().unwrap().is_end_stream() {
                    self.finish(BodyEnd::Complete);
                }
            }
            Poll::Ready(Some(Err(_))) => {
                self.finish(BodyEnd::Error);
                self.inner.take();
            }
            Poll::Ready(None) => {
                self.finish(BodyEnd::Complete);
                self.inner.take();
            }
            Poll::Pending => {}
        }
        frame
    }
    // Force a final poll for empty/chunked bodies so completion is observable.
    fn is_end_stream(&self) -> bool {
        self.finished
    }
    fn size_hint(&self) -> SizeHint {
        self.inner
            .as_ref()
            .map_or_else(SizeHint::default, |b| b.size_hint())
    }
}
