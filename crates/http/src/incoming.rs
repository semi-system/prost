//! Decoded request bodies shared by HTTP/1, HTTP/2 and HTTP/3 transports.
use crate::{Error, HttpFailure};
use bytes::Bytes;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{sync::mpsc, task::AbortHandle};

pub enum IncomingBody {
    Hyper(Incoming),
    Frames(Box<FrameBody>),
}
impl From<Incoming> for IncomingBody {
    fn from(body: Incoming) -> Self {
        Self::Hyper(body)
    }
}
impl IncomingBody {
    /// The producer must validate framing/declared length. Dropping a rejected
    /// or cancelled request aborts it, including while waiting on the network.
    pub fn frames(
        receiver: mpsc::Receiver<Result<Frame<Bytes>, Error>>,
        length: Option<u64>,
        producer: AbortHandle,
    ) -> Self {
        Self::Frames(Box::new(FrameBody {
            receiver,
            remaining: length,
            finished: false,
            producer,
            prefix: None,
        }))
    }
    /// Unlike Hyper's decoded headers, HTTP/3 does not carry END_STREAM on its
    /// HEADERS frame. Cache policy may inspect one bounded frame after ACLs to
    /// distinguish an empty GET from a streamed GET body without buffering it.
    pub(crate) async fn prepare_empty(&mut self, timeout_ms: u64) -> Result<(), HttpFailure> {
        let Self::Frames(body) = self else {
            return Ok(());
        };
        if body.finished || body.prefix.is_some() {
            return Ok(());
        }
        let receiving = body.receiver.recv();
        let frame = if timeout_ms == 0 {
            receiving.await
        } else {
            tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), receiving)
                .await
                .map_err(|_| HttpFailure::BodyTimeout)?
        };
        match frame {
            Some(Ok(frame)) => {
                body.prefix = Some(frame);
                Ok(())
            }
            Some(Err(error)) => {
                body.finished = true;
                Err(error
                    .downcast_ref::<HttpFailure>()
                    .copied()
                    .unwrap_or(HttpFailure::InvalidBody))
            }
            None => {
                body.finished = true;
                if body.remaining.is_some_and(|n| n > 0) {
                    Err(HttpFailure::InvalidBody)
                } else {
                    Ok(())
                }
            }
        }
    }
}
pub struct FrameBody {
    receiver: mpsc::Receiver<Result<Frame<Bytes>, Error>>,
    remaining: Option<u64>,
    finished: bool,
    producer: AbortHandle,
    prefix: Option<Frame<Bytes>>,
}
impl Drop for FrameBody {
    fn drop(&mut self) {
        self.producer.abort();
    }
}
impl Body for IncomingBody {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        match &mut *self {
            Self::Hyper(body) => Pin::new(body).poll_frame(cx).map(|frame| {
                frame.map(|r| r.map_err(|_| Box::new(HttpFailure::InvalidBody) as Error))
            }),
            Self::Frames(body) => {
                if body.finished {
                    return Poll::Ready(None);
                }
                let next = body.prefix.take().map_or_else(
                    || body.receiver.poll_recv(cx),
                    |frame| Poll::Ready(Some(Ok(frame))),
                );
                match next {
                    Poll::Ready(None) => {
                        body.finished = true;
                        if body.remaining.is_some_and(|n| n > 0) {
                            Poll::Ready(Some(Err(Box::new(HttpFailure::InvalidBody))))
                        } else {
                            Poll::Ready(None)
                        }
                    }
                    Poll::Ready(Some(Ok(frame))) => {
                        if let (Some(remaining), Some(data)) =
                            (&mut body.remaining, frame.data_ref())
                        {
                            *remaining = remaining.saturating_sub(data.len() as u64);
                        }
                        Poll::Ready(Some(Ok(frame)))
                    }
                    Poll::Ready(Some(Err(error))) => {
                        body.finished = true;
                        Poll::Ready(Some(Err(error)))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        match self {
            Self::Hyper(body) => body.is_end_stream(),
            Self::Frames(body) => {
                body.finished
                    || (body.prefix.is_none()
                        && body.receiver.is_closed()
                        && body.receiver.is_empty()
                        && body.remaining.is_none_or(|n| n == 0))
            }
        }
    }
    fn size_hint(&self) -> SizeHint {
        match self {
            Self::Hyper(body) => body.size_hint(),
            Self::Frames(body) => {
                if body.finished {
                    SizeHint::with_exact(0)
                } else {
                    body.remaining
                        .map_or_else(SizeHint::new, SizeHint::with_exact)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    #[tokio::test]
    async fn decoded_frames_preserve_trailers_detect_truncation_and_abort_producer() {
        let (tx, rx) = mpsc::channel(1);
        let producer = tokio::spawn(async move {
            tx.send(Ok(Frame::data(Bytes::from_static(b"abc"))))
                .await
                .unwrap();
            let mut trailers = http::HeaderMap::new();
            trailers.insert("x-end", http::HeaderValue::from_static("yes"));
            tx.send(Ok(Frame::trailers(trailers))).await.unwrap();
        });
        let mut body = IncomingBody::frames(rx, Some(3), producer.abort_handle());
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
        assert!(body.frame().await.is_none());
        let (tx, rx) = mpsc::channel(1);
        drop(tx);
        let producer = tokio::spawn(std::future::pending::<()>());
        let mut body = IncomingBody::frames(rx, Some(1), producer.abort_handle());
        assert!(!body.is_end_stream());
        assert_eq!(
            body.frame()
                .await
                .unwrap()
                .unwrap_err()
                .downcast_ref::<HttpFailure>(),
            Some(&HttpFailure::InvalidBody)
        );
        assert!(body.frame().await.is_none());
        assert!(body.is_end_stream());
        drop(body);
        assert!(producer.await.unwrap_err().is_cancelled());
    }
}
