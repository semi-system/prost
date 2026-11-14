use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::{Instant, Sleep},
};

/// A socket write-progress deadline. Allocates one timer on the first stall,
/// reuses it thereafter, and leaves reads independent of write backpressure.
pub struct TimedWrite<T> {
    inner: T,
    timeout: Duration,
    timer: Option<Pin<Box<Sleep>>>,
    armed: bool,
    failed: bool,
}
impl<T> TimedWrite<T> {
    pub fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            timeout,
            timer: None,
            armed: false,
            failed: false,
        }
    }
    pub fn get_ref(&self) -> &T {
        &self.inner
    }
    fn progress<U>(
        &mut self,
        result: Poll<io::Result<U>>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<U>> {
        if self.failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "socket write timeout",
            )));
        }
        match result {
            Poll::Ready(value) => {
                self.armed = false;
                Poll::Ready(value)
            }
            Poll::Pending => {
                if !self.armed {
                    let deadline = Instant::now() + self.timeout;
                    if let Some(timer) = &mut self.timer {
                        timer.as_mut().reset(deadline);
                    } else {
                        self.timer = Some(Box::pin(tokio::time::sleep_until(deadline)));
                    }
                    self.armed = true;
                }
                if self.timer.as_mut().unwrap().as_mut().poll(cx).is_ready() {
                    self.failed = true;
                    Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "socket write timeout",
                    )))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for TimedWrite<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for TimedWrite<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = if self.failed {
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_write(cx, buf)
        };
        self.progress(result, cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = if self.failed {
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
        };
        self.progress(result, cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = if self.failed {
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_flush(cx)
        };
        self.progress(result, cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = if self.failed {
            Poll::Pending
        } else {
            Pin::new(&mut self.inner).poll_shutdown(cx)
        };
        self.progress(result, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test(start_paused = true)]
    async fn stalled_write_times_out_but_reads_remain_independent() {
        let (io, mut peer) = tokio::io::duplex(8);
        let mut io = TimedWrite::new(io, Duration::from_millis(50));
        io.write_all(b"12345678").await.unwrap();
        assert_eq!(
            io.write_all(b"x").await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        peer.write_all(b"ok").await.unwrap();
        let mut buf = [0; 2];
        io.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ok");
        assert!(io.flush().await.is_err());
    }
    #[tokio::test(start_paused = true)]
    async fn successful_progress_resets_deadline() {
        let (io, mut peer) = tokio::io::duplex(1);
        let mut io = TimedWrite::new(io, Duration::from_millis(50));
        io.write_all(b"a").await.unwrap();
        let writer = tokio::spawn(async move { io.write_all(b"bc").await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let mut buf = [0];
        peer.read_exact(&mut buf).await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        peer.read_exact(&mut buf).await.unwrap();
        writer.await.unwrap().unwrap();
    }
}
