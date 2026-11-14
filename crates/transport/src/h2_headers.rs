use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    time::Sleep,
};

/// Observe framing only; Hyper retains HPACK and all protocol validation.
/// A CONTINUATION block cannot legally interleave another stream, so expiration
/// closes the connection rather than injecting a frame into Hyper's state.
pub struct H2HeaderDeadline<T> {
    inner: T,
    parser: Parser,
    timer: Option<Pin<Box<Sleep>>>,
    epoch: u64,
    timeout: Duration,
}
impl<T> H2HeaderDeadline<T> {
    pub fn new(inner: T, timeout: Duration) -> Self {
        Self {
            inner,
            parser: Parser::default(),
            timer: None,
            epoch: 0,
            timeout,
        }
    }
}
#[derive(Default)]
struct Parser {
    preface: usize,
    disabled: bool,
    header: [u8; 9],
    have: usize,
    left: usize,
    block: bool,
    end_block: bool,
    epoch: u64,
}
impl Parser {
    fn active(&self) -> bool {
        self.block || self.have > 0
    }
    fn feed(&mut self, mut data: &[u8]) {
        const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
        if self.disabled {
            return;
        }
        while !data.is_empty() {
            if self.preface < PREFACE.len() {
                if data[0] != PREFACE[self.preface] {
                    self.disabled = true;
                    return;
                }
                self.preface += 1;
                data = &data[1..];
                continue;
            }
            if self.left > 0 {
                let n = self.left.min(data.len());
                self.left -= n;
                data = &data[n..];
                if self.left == 0 && self.end_block {
                    self.block = false;
                    self.end_block = false;
                }
                continue;
            }
            if self.have == 0 && !self.block {
                self.epoch = self.epoch.wrapping_add(1);
            }
            let n = (9 - self.have).min(data.len());
            self.header[self.have..self.have + n].copy_from_slice(&data[..n]);
            self.have += n;
            data = &data[n..];
            if self.have == 9 {
                self.left = ((self.header[0] as usize) << 16)
                    | ((self.header[1] as usize) << 8)
                    | self.header[2] as usize;
                if matches!(self.header[3], 1 | 9) {
                    self.block = true;
                    self.end_block = self.header[4] & 4 != 0;
                }
                self.have = 0;
                if self.left == 0 && self.end_block {
                    self.block = false;
                    self.end_block = false;
                }
            }
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for H2HeaderDeadline<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self
            .timer
            .as_mut()
            .is_some_and(|t| t.as_mut().poll(cx).is_ready())
        {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "H2 header block deadline",
            )));
        }
        let start = buf.filled().len();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &result {
            self.parser.feed(&buf.filled()[start..]);
            if self.parser.active() && !self.parser.disabled {
                if self.timer.is_none() || self.epoch != self.parser.epoch {
                    self.epoch = self.parser.epoch;
                    self.timer = Some(Box::pin(tokio::time::sleep(self.timeout)));
                }
                let _ = self.timer.as_mut().unwrap().as_mut().poll(cx);
            } else {
                self.timer = None;
            }
        }
        result
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for H2HeaderDeadline<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, buf)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
