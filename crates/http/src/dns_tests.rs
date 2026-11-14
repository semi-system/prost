use super::*;
use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use hyper::{body::Frame, service::service_fn};
use prost_transport::{DnsLookup, DnsResolver, LookupFuture};
use std::{
    convert::Infallible,
    net::IpAddr,
    sync::atomic::{AtomicU8, Ordering},
};
use tokio::{net::TcpListener, sync::Notify};
struct Lookup(Arc<AtomicU8>);
impl DnsLookup for Lookup {
    fn lookup(&self, _: String, _: usize) -> LookupFuture {
        let n = self.0.load(Ordering::Relaxed);
        Box::pin(async move {
            Ok(if n == 0 {
                vec![]
            } else {
                vec![IpAddr::from([127, 0, 0, n])]
            })
        })
    }
}
async fn backend(listener: TcpListener, label: &'static str, gate: Arc<Notify>) {
    while let Ok((io, _)) = listener.accept().await {
        let gate = gate.clone();
        tokio::spawn(async move {
            let service =
                service_fn(move |request: Request<Incoming>| {
                    let gate = gate.clone();
                    async move {
                        let hold = request.uri().path() == "/hold";
                        let first = futures_util::stream::once(async move {
                            Ok::<_, Infallible>(Frame::data(Bytes::from_static(label.as_bytes())))
                        });
                        let last = futures_util::stream::once(async move {
                            if hold {
                                gate.notified().await;
                            }
                            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"end")))
                        });
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header(
                                    "x-host",
                                    request.headers().get(header::HOST).cloned().unwrap_or_else(
                                        || http::HeaderValue::from_static("h2-authority"),
                                    ),
                                )
                                .body(http_body_util::StreamBody::new(first.chain(last)))
                                .unwrap(),
                        )
                    }
                });
            let _ =
                hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                    .serve_connection(TokioIo::new(io), service)
                    .await;
        });
    }
}
#[tokio::test]
async fn changed_dns_rotates_h1_h2_pools_without_interrupting_old_streams() -> Result<()> {
    for h2 in [false, true] {
        let a = TcpListener::bind("127.0.0.1:0").await?;
        let port = a.local_addr()?.port();
        let b = TcpListener::bind(("127.0.0.2", port)).await?;
        let gate = Arc::new(Notify::new());
        let a_task = tokio::spawn(backend(a, "a", gate.clone()));
        let b_task = tokio::spawn(backend(b, "b", gate.clone()));
        let ips = Arc::new(AtomicU8::new(1));
        let dns = DnsResolver::with_lookup(
            prost_config::DnsPolicy {
                ttl_ms: 100,
                negative_ttl_ms: 100,
                ..Default::default()
            },
            Arc::new(Lookup(ips.clone())),
        )?;
        let config=prost_config::Config::parse(&format!("schema_version=1\nlisteners=[]\n[upstreams.web]\nhttp2={h2}\n[[upstreams.web.endpoints]]\nid='one'\naddress='rotate.test:{port}'\n"))?;
        let upstream = &config.upstreams["web"];
        let pool = ClientPool::new_with_dns(
            upstream,
            prost_transport::client_tls(None)?,
            1,
            Arc::new(PoolStats::new(1)),
            Some(dns.clone()),
        )?;
        let request = |path: &str| {
            Request::builder()
                .uri(format!("http://rotate.test:{port}{path}"))
                .body(RequestBody::Empty)
                .unwrap()
        };
        dns.resolve("rotate.test").await?;
        let old = pool.current_dns()?;
        let mut held = old.request(request("/hold")).await?.into_body();
        assert_eq!(held.frame().await.unwrap()?.into_data().unwrap(), "a");
        let fresh = old.request(request("/")).await?;
        if !h2 {
            assert_eq!(fresh.headers()["x-host"], format!("rotate.test:{port}"));
        }
        assert_eq!(fresh.into_body().collect().await?.to_bytes(), "aend");
        ips.store(2, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(110)).await;
        dns.resolve("rotate.test").await?;
        drop(old);
        let new = pool.current_dns()?;
        assert_eq!(
            new.request(request("/"))
                .await?
                .into_body()
                .collect()
                .await?
                .to_bytes(),
            "bend"
        );
        ips.store(0, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(110)).await;
        assert!(dns.resolve("rotate.test").await.is_err());
        assert!(pool.current_dns()?.request(request("/")).await.is_err());
        gate.notify_one();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), held.collect())
                .await??
                .to_bytes(),
            "end"
        );
        a_task.abort();
        b_task.abort();
    }
    Ok(())
}
