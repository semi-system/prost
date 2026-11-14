use crate::{Body, Error};
use bytes::Bytes;
use hyper::{
    body::{Body as HttpBody, Frame, SizeHint},
    rt::Executor,
};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

tokio::task_local! {static WRITE:Arc<Watch>;}
struct State {
    sequence: u64,
    pending: VecDeque<(u64, Instant)>,
}
struct Watch {
    state: Mutex<State>,
    changed: Notify,
    body_waker: futures_util::task::AtomicWaker,
    timeout: Duration,
}
impl Watch {
    fn start(&self) -> u64 {
        let mut s = self.state.lock().unwrap();
        s.sequence = s.sequence.wrapping_add(1);
        let id = s.sequence;
        s.pending.push_back((id, Instant::now() + self.timeout));
        drop(s);
        self.changed.notify_one();
        id
    }
    fn end(&self, id: u64) {
        let mut s = self.state.lock().unwrap();
        if let Some(index) = s.pending.iter().position(|(key, _)| *key == id) {
            s.pending.remove(index);
        }
        drop(s);
        self.changed.notify_one();
        self.body_waker.wake();
    }
    fn drained(&self, cx: &Context<'_>) -> bool {
        self.body_waker.register(cx.waker());
        self.state.lock().unwrap().pending.is_empty()
    }
    async fn expired(&self) {
        loop {
            let next = self.state.lock().unwrap().pending.front().copied();
            if let Some((id, deadline)) = next {
                tokio::select! {_=tokio::time::sleep_until(deadline)=>{
                    if self.state.lock().unwrap().pending.front().is_some_and(|(key,d)|*key==id && *d<=Instant::now()){return;}
                },_=self.changed.notified()=>{}}
            } else {
                self.changed.notified().await;
            }
        }
    }
}
#[derive(Clone)]
pub struct H2Executor {
    timeout: Option<Duration>,
    counter: Arc<AtomicU64>,
}
impl H2Executor {
    pub fn new(ms: u64, counter: Arc<AtomicU64>) -> Self {
        Self {
            timeout: (ms > 0).then(|| Duration::from_millis(ms)),
            counter,
        }
    }
}
impl<F> Executor<F> for H2Executor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        if let Some(timeout) = self.timeout {
            let watch = Arc::new(Watch {
                state: Mutex::new(State {
                    sequence: 0,
                    pending: VecDeque::new(),
                }),
                changed: Notify::new(),
                body_waker: Default::default(),
                timeout,
            });
            let counter = self.counter.clone();
            let expired = watch.clone();
            tokio::spawn(WRITE.scope(watch,async move{tokio::select!{_=future=>{},_=expired.expired()=>{counter.fetch_add(1,Ordering::Relaxed);}}}));
        } else {
            tokio::spawn(future);
        }
    }
}
pub fn observe_response(response: &mut http::Response<Body>) {
    if response.body().is_end_stream() {
        return;
    }
    if let Ok(watch) = WRITE.try_with(Arc::clone) {
        let old = std::mem::replace(response.body_mut(), Body::text(""));
        *response.body_mut() = Body::custom(old, |body| Progress {
            body: Box::pin(body),
            watch,
            buffer: None,
            trailers: None,
            source_done: false,
            ended: false,
        });
    }
}
// Owner Drop follows clones/slices retained inside h2's DATA queue. The Body
// cannot signal EOS until every owner drains, keeping Hyper's real per-stream
// future/reset handle alive after the producer reaches EOF.
struct DataOwner {
    data: Bytes,
    watch: Arc<Watch>,
    id: u64,
}
impl AsRef<[u8]> for DataOwner {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}
impl Drop for DataOwner {
    fn drop(&mut self) {
        self.watch.end(self.id);
    }
}
struct Progress {
    body: Pin<Box<Body>>,
    watch: Arc<Watch>,
    buffer: Option<Bytes>,
    trailers: Option<http::HeaderMap>,
    source_done: bool,
    ended: bool,
}
impl HttpBody for Progress {
    type Data = Bytes;
    type Error = Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        loop {
            if let Some(data) = &mut self.buffer {
                if !data.is_empty() {
                    let bytes = data.split_to(data.len().min(16384));
                    let id = self.watch.start();
                    return Poll::Ready(Some(Ok(Frame::data(Bytes::from_owner(DataOwner {
                        data: bytes,
                        watch: self.watch.clone(),
                        id,
                    })))));
                }
                self.buffer = None;
            }
            if self.source_done {
                if !self.watch.drained(cx) {
                    return Poll::Pending;
                }
                self.ended = true;
                return Poll::Ready(self.trailers.take().map(|t| Ok(Frame::trailers(t))));
            }
            match self.body.as_mut().poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Err(e))) => {
                    self.ended = true;
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => self.source_done = true,
                Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                    Ok(data) => self.buffer = Some(data),
                    Err(frame) => {
                        if let Ok(t) = frame.into_trailers() {
                            self.trailers = Some(t);
                            self.source_done = true;
                        }
                    }
                },
            }
        }
    }
    fn is_end_stream(&self) -> bool {
        self.ended
    }
    fn size_hint(&self) -> SizeHint {
        let mut hint = self.body.size_hint();
        let buffered = self.buffer.as_ref().map_or(0, |b| b.len() as u64);
        if let Some(exact) = hint.exact() {
            hint.set_exact(exact.saturating_add(buffered));
        } else {
            hint.set_lower(hint.lower().saturating_add(buffered));
        }
        hint
    }
}
