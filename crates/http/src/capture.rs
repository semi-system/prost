use crate::pool_stats::{Activity, ConnectionTag};
use http::Response;
use hyper::body::Incoming;
use hyper_util::client::legacy::{connect::CaptureConnection, Error, ResponseFuture};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Public hyper-util capture API observes assignment before response headers,
/// including calls canceled while the backend is still producing headers.
pub(crate) struct Captured {
    future: ResponseFuture,
    capture: CaptureConnection,
    activity: Option<Arc<Activity>>,
}
impl Captured {
    pub fn new(future: ResponseFuture, capture: CaptureConnection) -> Self {
        Self {
            future,
            capture,
            activity: None,
        }
    }
}
impl Future for Captured {
    type Output = Result<Response<Incoming>, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut result = Pin::new(&mut self.future).poll(cx);
        if self.activity.is_none() {
            let tag = {
                let metadata = self.capture.connection_metadata();
                metadata.as_ref().and_then(|m| {
                    let mut extra = http::Extensions::new();
                    m.get_extras(&mut extra);
                    extra.remove::<ConnectionTag>()
                })
            };
            if let Some(tag) = tag {
                self.activity = Some(tag.activate());
            }
        }
        if let Poll::Ready(Ok(response)) = &mut result {
            if let Some(activity) = self.activity.take() {
                response.extensions_mut().insert(activity);
            }
        }
        result
    }
}
