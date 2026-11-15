//! Low-level HTTP/1 test client that honors dispatcher backpressure for every
//! request, including bodyless/cache responses that finish before driver reuse.
use http::{Request, Response};
use hyper::{
    body::{Body, Incoming},
    client::conn::http1,
    rt::{Read, Write},
};
use std::error::Error;

pub(super) struct Sender<B> {
    inner: http1::SendRequest<B>,
}
impl<B: Body + 'static> Sender<B> {
    pub async fn ready(&mut self) -> hyper::Result<()> {
        self.inner.ready().await
    }
    pub async fn send_request(&mut self, request: Request<B>) -> hyper::Result<Response<Incoming>> {
        self.inner.ready().await?;
        self.inner.send_request(request).await
    }
}
pub(super) async fn handshake<T, B>(io: T) -> hyper::Result<(Sender<B>, http1::Connection<T, B>)>
where
    T: Read + Write + Unpin,
    B: Body + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    let (inner, connection) = http1::handshake(io).await?;
    Ok((Sender { inner }, connection))
}

#[tokio::test]
async fn queued_request_waits_for_dispatcher_then_reuses_the_same_connection() -> anyhow::Result<()>
{
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::{convert::Infallible, time::Duration};

    let (client_io, server_io) = tokio::io::duplex(4096);
    let (mut sender, connection) = handshake(TokioIo::new(client_io)).await?;
    let request = |path: &str| {
        Request::builder()
            .uri(path)
            .header("host", "test")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };

    // Occupy Hyper's initial dispatch slot before polling either IO driver.
    // A direct second send_request fails immediately with "not ready" here.
    let first = sender.inner.send_request(request("/first"));
    assert!(!sender.inner.is_ready());
    let rejected = sender
        .inner
        .send_request(request("/not-ready"))
        .await
        .unwrap_err();
    assert!(rejected.is_canceled());
    let mut second = Box::pin(sender.send_request(request("/second")));
    assert!(futures_util::poll!(second.as_mut()).is_pending());

    let server = tokio::spawn(async move {
        hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(server_io),
                service_fn(|request: Request<Incoming>| async move {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(
                        request.uri().path().to_owned(),
                    ))))
                }),
            )
            .await
    });
    let driver = tokio::spawn(connection);
    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(
            first.await?.into_body().collect().await?.to_bytes(),
            "/first"
        );
        assert_eq!(
            second
                .as_mut()
                .await?
                .into_body()
                .collect()
                .await?
                .to_bytes(),
            "/second"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    drop(second);
    drop(sender);
    driver.abort();
    server.abort();
    Ok(())
}
