use crate::pool_stats::{ConnectionTag, PoolStats, SocketLease};
use crate::Error;
use http::Uri;
use hyper::rt::{Read, ReadBufCursor, Write};
use hyper_util::{
    client::legacy::connect::{Connected, Connection, HttpConnector},
    rt::TokioIo,
};
use prost_config::Upstream;
use prost_transport::TimedWrite;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tower_service::Service;

type Base = hyper_rustls::HttpsConnector<ProxyTcp>;
type BaseIo = <Base as Service<Uri>>::Response;

#[derive(Clone)]
pub(crate) struct ProxyTcp {
    pub inner: HttpConnector,
    pub addresses: Option<prost_transport::ProxyAddresses>,
    pub required: bool,
    pub dns: Option<Arc<prost_transport::DnsResolver>>,
}
impl Service<Uri> for ProxyTcp {
    type Response = TokioIo<tokio::net::TcpStream>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        if self.required && self.addresses.is_none() {
            return Box::pin(async {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "PROXY v2 HTTP client requires frontend connection identity",
                )
                .into())
            });
        }
        let future = self.inner.call(uri.clone());
        let dns = self.dns.clone();
        let addresses = self.addresses;
        Box::pin(async move {
            let mut io = if let Some(dns) = dns {
                let host = uri.host().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing DNS endpoint host")
                })?;
                let port = uri
                    .port_u16()
                    .unwrap_or(if uri.scheme_str() == Some("https") {
                        443
                    } else {
                        80
                    });
                let address = if host.starts_with('[') || !host.contains(':') {
                    format!("{host}:{port}")
                } else {
                    format!("[{host}]:{port}")
                };
                let tcp = dns.connect(&address).await?;
                prost_transport::configure_socket(&tcp)?;
                TokioIo::new(tcp)
            } else {
                future.await?
            };
            if let Some(addresses) = addresses {
                prost_transport::write_proxy_v2(io.inner_mut(), addresses).await?;
            }
            Ok(io)
        })
    }
}

#[derive(Clone)]
pub struct Connector {
    capacity: Option<Arc<tokio::sync::Semaphore>>,
    inner: Base,
    connect_timeout: Duration,
    write_timeout: Option<Duration>,
    stats: Arc<PoolStats>,
    worker: usize,
    observe_reuse: bool,
    observe_state: bool,
}
impl Connector {
    pub(crate) fn new(
        inner: Base,
        config: &Upstream,
        stats: Arc<PoolStats>,
        worker: usize,
        capacity: Option<Arc<tokio::sync::Semaphore>>,
    ) -> Self {
        Self {
            capacity,
            stats,
            worker,
            observe_reuse: config.observe_connection_reuse || config.observe_pool_state,
            observe_state: config.observe_pool_state,
            inner,
            connect_timeout: Duration::from_millis(config.connect_timeout_ms),
            write_timeout: config
                .write_timeout_ms
                .filter(|ms| *ms > 0)
                .map(Duration::from_millis),
        }
    }
}
impl Service<Uri> for Connector {
    type Response = TrackedIo;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<TrackedIo, Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, uri: Uri) -> Self::Future {
        let mut attempt = self
            .stats
            .attempt(self.worker, uri.scheme_str() == Some("https"));
        let future = self.inner.call(uri);
        let connect_timeout = self.connect_timeout;
        let write_timeout = self.write_timeout;
        let observe_reuse = self.observe_reuse;
        let observe_state = self.observe_state;
        let capacity = self.capacity.clone();
        Box::pin(async move {
            let capacity = match capacity.map(|c| c.try_acquire_owned()).transpose() {
                Ok(permit) => permit,
                Err(_) => return Err(Box::new(crate::ProxyCapacity) as Error),
            };
            // Includes DNS, TCP and TLS, rather than TCP establishment alone.
            let io = match tokio::time::timeout(connect_timeout, future).await {
                Ok(Ok(io)) => io,
                Ok(Err(error)) => {
                    attempt.error(false);
                    return Err(error);
                }
                Err(_) => {
                    attempt.error(true);
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "upstream connect/TLS timeout",
                    )
                    .into());
                }
            };
            // MaybeHttpsStream embeds >1 KiB of TLS state even for plain TCP.
            // Keep the common socket compact and retain the original TLS
            // connection metadata (ALPN/HttpInfo) behind one cold allocation.
            let io = match io {
                hyper_rustls::MaybeHttpsStream::Http(tcp) => Io::Plain(tcp),
                tls => Io::Secure(Box::new(tls)),
            };
            let inner = match write_timeout {
                Some(timeout) => Io::Timed(Box::new(TokioIo::new(TimedWrite::new(
                    TokioIo::new(io),
                    timeout,
                )))),
                None => io,
            };
            let (lease, tag) = attempt.connected(observe_reuse, observe_state);
            Ok(TrackedIo {
                _capacity: capacity,
                inner,
                _lease: lease,
                tag,
            })
        })
    }
}

pub struct TrackedIo {
    _capacity: Option<tokio::sync::OwnedSemaphorePermit>,
    inner: Io,
    _lease: SocketLease,
    tag: Option<ConnectionTag>,
}
impl Connection for TrackedIo {
    fn connected(&self) -> Connected {
        let connected = self.inner.connected();
        match &self.tag {
            Some(tag) => connected.extra(tag.clone()),
            None => connected,
        }
    }
}
impl Read for TrackedIo {
    #[inline]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl Write for TrackedIo {
    #[inline]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    #[inline]
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
    #[inline]
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub enum Io {
    Plain(TokioIo<tokio::net::TcpStream>),
    Secure(Box<BaseIo>),
    // The optional timer must not enlarge every pooled plain connection.
    Timed(Box<TokioIo<TimedWrite<TokioIo<Io>>>>),
}
impl Connection for Io {
    fn connected(&self) -> Connected {
        match self {
            Self::Plain(io) => io.connected(),
            Self::Secure(io) => io.connected(),
            Self::Timed(io) => io.inner().get_ref().inner().connected(),
        }
    }
}
impl Read for Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(io) => Pin::new(io).poll_read(cx, buf),
            Self::Secure(io) => Pin::new(io.as_mut()).poll_read(cx, buf),
            Self::Timed(io) => Pin::new(io.as_mut()).poll_read(cx, buf),
        }
    }
}
impl Write for Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(io) => Pin::new(io).poll_write(cx, buf),
            Self::Secure(io) => Pin::new(io.as_mut()).poll_write(cx, buf),
            Self::Timed(io) => Pin::new(io.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(io) => Pin::new(io).poll_write_vectored(cx, bufs),
            Self::Secure(io) => Pin::new(io.as_mut()).poll_write_vectored(cx, bufs),
            Self::Timed(io) => Pin::new(io.as_mut()).poll_write_vectored(cx, bufs),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(io) => io.is_write_vectored(),
            Self::Secure(io) => io.is_write_vectored(),
            Self::Timed(io) => io.is_write_vectored(),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(io) => Pin::new(io).poll_flush(cx),
            Self::Secure(io) => Pin::new(io.as_mut()).poll_flush(cx),
            Self::Timed(io) => Pin::new(io.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(io) => Pin::new(io).poll_shutdown(cx),
            Self::Secure(io) => Pin::new(io.as_mut()).poll_shutdown(cx),
            Self::Timed(io) => Pin::new(io.as_mut()).poll_shutdown(cx),
        }
    }
}
