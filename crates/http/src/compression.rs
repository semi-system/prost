use crate::{Body, Error};
use async_compression::tokio::bufread::{BrotliEncoder, GzipEncoder};
use bytes::Bytes;
use futures_util::Stream;
use http::{header, HeaderMap, HeaderValue, Method, Response};
use hyper::body::{Body as HttpBody, Frame};
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::io::{ReaderStream, StreamReader};

pub(crate) struct Compressor {
    pub config: prost_config::Compression,
    capacity: Arc<Semaphore>,
}
impl Compressor {
    pub fn new(config: prost_config::Compression) -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
        }
    }
    pub fn apply(&self, method: &Method, accepted: &str, response: &mut Response<Body>) {
        let headers = response.headers();
        if method == Method::HEAD
            || response.status() != 200
            || headers.contains_key(header::CONTENT_ENCODING)
            || headers.contains_key(header::CONTENT_RANGE)
            || headers.get_all(header::CACHE_CONTROL).iter().any(|h| {
                h.to_str().is_ok_and(|s| {
                    s.split(',')
                        .any(|s| s.trim().eq_ignore_ascii_case("no-transform"))
                })
            })
        {
            return;
        }
        let content = headers
            .get(header::CONTENT_TYPE)
            .and_then(|h| h.to_str().ok())
            .unwrap_or("");
        if content.starts_with("text/event-stream")
            || content.starts_with("application/grpc")
            || !self
                .config
                .content_types
                .iter()
                .any(|p| content.starts_with(p))
        {
            return;
        }
        // Vary applies even for clients preferring identity or exhausted budget.
        if !headers.get_all(header::VARY).iter().any(|h| {
            h.to_str().is_ok_and(|s| {
                s.split(',')
                    .any(|s| s.trim().eq_ignore_ascii_case("accept-encoding") || s.trim() == "*")
            })
        }) {
            response
                .headers_mut()
                .append(header::VARY, HeaderValue::from_static("Accept-Encoding"));
        }
        let identity = identity_quality(accepted) > 0.;
        let Some(encoding) = negotiate(accepted, &self.config.algorithms) else {
            if !identity {
                *response = crate::response(406, "no acceptable content encoding\n");
            }
            return;
        };
        if identity
            && response
                .body()
                .size_hint()
                .exact()
                .is_some_and(|n| n < self.config.min_bytes)
        {
            return;
        }
        let Ok(permit) = self.capacity.clone().try_acquire_owned() else {
            if !identity {
                *response = crate::response(503, "compression capacity exhausted\n");
            }
            return;
        };
        let headers = response.headers_mut();
        headers.remove(header::CONTENT_LENGTH);
        headers.remove(header::ACCEPT_RANGES);
        headers.remove("content-md5");
        headers.remove("digest");
        headers.remove("content-digest");
        headers.remove("repr-digest");
        if let Some(etag) = headers
            .get(header::ETAG)
            .filter(|v| !v.as_bytes().starts_with(b"W/"))
            .cloned()
        {
            if let Ok(value) = HeaderValue::from_str(&format!("W/{}", etag.to_str().unwrap_or("")))
            {
                headers.insert(header::ETAG, value);
            }
        }
        headers.insert(
            header::CONTENT_ENCODING,
            HeaderValue::from_static(if encoding == "br" { "br" } else { "gzip" }),
        );
        let old = std::mem::replace(response.body_mut(), Body::text(""));
        let level = async_compression::Level::Precise(self.config.level as i32);
        *response.body_mut() = Body::custom(old, |body| {
            let trailers = Arc::new(Mutex::new(None));
            let stream = Source {
                body: Box::pin(body),
                trailers: trailers.clone(),
            };
            let reader = StreamReader::new(stream);
            let reader: Pin<Box<dyn tokio::io::AsyncRead + Send>> = if encoding == "br" {
                Box::pin(BrotliEncoder::with_quality(reader, level))
            } else {
                Box::pin(GzipEncoder::with_quality(reader, level))
            };
            Encoded {
                data: ReaderStream::with_capacity(reader, 16384),
                trailers,
                ended: false,
                _permit: permit,
            }
        });
    }
}
fn negotiate<'a>(header: &str, algorithms: &'a [String]) -> Option<&'a str> {
    let mut best = (0., None);
    for algorithm in algorithms {
        let mut explicit = None;
        let mut wildcard = None;
        for item in header.split(',') {
            let mut parts = item.trim().split(';');
            let name = parts.next().unwrap_or("").trim();
            let mut quality = 1f32;
            for part in parts {
                if let Some(q) = part.trim().strip_prefix("q=") {
                    quality = q
                        .parse::<f32>()
                        .ok()
                        .filter(|q| q.is_finite() && (0. ..=1.).contains(q))
                        .unwrap_or(0.);
                }
            }
            if name.eq_ignore_ascii_case(algorithm) {
                explicit = Some(quality);
            }
            if name == "*" {
                wildcard = Some(quality);
            }
        }
        let quality = explicit.or(wildcard).unwrap_or(0.);
        if quality > best.0 {
            best = (quality, Some(algorithm.as_str()));
        }
    }
    if explicit_identity(header).is_some_and(|q| q > best.0) {
        None
    } else {
        best.1
    }
}
fn explicit_identity(header: &str) -> Option<f32> {
    header.split(',').find_map(|item| {
        let mut parts = item.trim().split(';');
        if !parts.next()?.trim().eq_ignore_ascii_case("identity") {
            return None;
        }
        Some(
            parts
                .find_map(|p| p.trim().strip_prefix("q="))
                .map_or(1., |q| {
                    q.parse::<f32>()
                        .ok()
                        .filter(|q| q.is_finite() && (0. ..=1.).contains(q))
                        .unwrap_or(0.)
                }),
        )
    })
}
fn identity_quality(header: &str) -> f32 {
    explicit_identity(header).unwrap_or_else(|| {
        if header.split(',').any(|i| {
            let mut p = i.trim().split(';');
            p.next().unwrap_or("").trim() == "*"
                && p.any(|s| {
                    s.trim().split_once('=').is_some_and(|(k, v)| {
                        k.trim().eq_ignore_ascii_case("q") && v.trim().parse::<f32>() == Ok(0.)
                    })
                })
        }) {
            0.
        } else {
            1.
        }
    })
}
struct Source {
    body: Pin<Box<Body>>,
    trailers: Arc<Mutex<Option<HeaderMap>>>,
}
impl Stream for Source {
    type Item = Result<Bytes, io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.body.as_mut().poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => return Poll::Ready(Some(Ok(data))),
                    Err(frame) => {
                        if let Ok(headers) = frame.into_trailers() {
                            *self.trailers.lock().unwrap() = Some(headers);
                        }
                    }
                },
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(io::Error::other(error))))
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
struct Encoded {
    data: ReaderStream<Pin<Box<dyn tokio::io::AsyncRead + Send>>>,
    trailers: Arc<Mutex<Option<HeaderMap>>>,
    ended: bool,
    _permit: OwnedSemaphorePermit,
}
impl HttpBody for Encoded {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.data).poll_next(cx) {
            Poll::Ready(Some(Ok(data))) => Poll::Ready(Some(Ok(Frame::data(data)))),
            Poll::Ready(Some(Err(e))) => {
                self.ended = true;
                Poll::Ready(Some(Err(e.into())))
            }
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(
                    self.trailers
                        .lock()
                        .unwrap()
                        .take()
                        .map(|t| Ok(Frame::trailers(t))),
                )
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.ended
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quality_and_explicit_zero_override_wildcard() {
        let order = vec!["br".into(), "gzip".into()];
        assert_eq!(negotiate("gzip;q=0.8, br;q=0.2", &order), Some("gzip"));
        assert_eq!(negotiate("*;q=1,br;q=0", &order), Some("gzip"));
        assert_eq!(negotiate("gzip;q=NaN,br;q=2", &order), None);
    }
}
