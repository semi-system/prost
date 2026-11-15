use super::*;
use futures_util::StreamExt;
use http::{Response, StatusCode};
use http_body_util::{Full, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use std::{
    convert::Infallible,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::net::TcpListener;
const SIGNED_QUERY: &str =
    "X-Amz-Credential=example%2Fscope&X-Amz-Security-Token=A%2Bb%2Fc%3D&X-Amz-Signature=example";

async fn server(
    tls: Option<Arc<rustls::ServerConfig>>,
) -> Result<(u16, Arc<AtomicUsize>, tokio::task::JoinHandle<()>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let tls = tls.clone();
            let count = count.clone();
            tokio::spawn(async move {
                let io: prost_transport::BoxStream = if let Some(tls) = tls {
                    match tokio_rustls::TlsAcceptor::from(tls).accept(io).await {
                        Ok(io) => Box::new(io),
                        Err(_) => return,
                    }
                } else {
                    Box::new(io)
                };
                let service = service_fn(move |request: Request<Incoming>| {
                    count.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if request.uri().path() == "/slow" {
                            std::future::pending::<()>().await;
                        }
                        let body = if request.uri().path() == "/chunks" {
                            StreamBody::new(futures_util::stream::iter([b"1234", b"5678"]).map(
                                |s| Ok::<_, Infallible>(Frame::data(Bytes::copy_from_slice(s))),
                            ))
                            .boxed_unsync()
                        } else {
                            Full::new(Bytes::from_static(b"PEM-data")).boxed_unsync()
                        };
                        let mut response = Response::new(body);
                        match request.uri().path() {
                            "/presign" => {
                                *response.status_mut() = StatusCode::TEMPORARY_REDIRECT;
                                response.headers_mut().insert(
                                    header::LOCATION,
                                    format!("/signed?{SIGNED_QUERY}").parse().unwrap(),
                                );
                            }
                            "/signed" if request.uri().query() != Some(SIGNED_QUERY) => {
                                *response.status_mut() = StatusCode::FORBIDDEN
                            }
                            "/redirect" => {
                                *response.status_mut() = StatusCode::FOUND;
                                response
                                    .headers_mut()
                                    .insert(header::LOCATION, "/ok".parse().unwrap());
                            }
                            "/loop" => {
                                *response.status_mut() = StatusCode::FOUND;
                                response
                                    .headers_mut()
                                    .insert(header::LOCATION, "/loop".parse().unwrap());
                            }
                            "/downgrade" => {
                                *response.status_mut() = StatusCode::FOUND;
                                response.headers_mut().insert(
                                    header::LOCATION,
                                    "http://127.0.0.1/ok".parse().unwrap(),
                                );
                            }
                            "/error" => *response.status_mut() = StatusCode::NOT_FOUND,
                            _ => {}
                        }
                        Ok::<_, Infallible>(response)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    Ok((port, calls, task))
}
#[tokio::test]
async fn remote_dedup_redirect_status_size_stream_timeout_and_redaction() -> Result<()> {
    let (port, calls, task) = server(None).await?;
    let source = format!("http://127.0.0.1:{port}");
    let target = format!("{source}/ok");
    tokio::task::spawn_blocking(move || {
        let mut fetch = Fetcher::default();
        let a = fetch.get(&target)?;
        let b = fetch.get(&target)?;
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.as_slice(), b"PEM-data");
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let client = client(prost_transport::client_tls(None)?)?;
    assert_eq!(
        download(&client, &format!("{source}/presign"), LIMIT).await?,
        b"PEM-data"
    );
    assert_eq!(
        download(&client, &format!("{source}/redirect"), LIMIT).await?,
        b"PEM-data"
    );
    assert!(download(&client, &format!("{source}/loop"), LIMIT)
        .await
        .unwrap_err()
        .to_string()
        .contains("redirect limit"));
    let error = download(
        &client,
        &format!("{source}/error?token=private-token"),
        LIMIT,
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("404"));
    assert!(!format!("{error:#}").contains("private-token"));
    assert!(download(&client, &format!("{source}/ok"), 3)
        .await
        .unwrap_err()
        .to_string()
        .contains("budget"));
    assert!(download(&client, &format!("{source}/chunks"), 5)
        .await
        .unwrap_err()
        .to_string()
        .contains("budget"));
    assert!(tokio::time::timeout(
        Duration::from_millis(30),
        download(&client, &format!("{source}/slow"), LIMIT)
    )
    .await
    .is_err());
    task.abort();
    Ok(())
}
#[tokio::test]
async fn https_verifies_trust_hostname_and_rejects_downgrade() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let key = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let cert = dir.path().join("cert.pem");
    let private_key = dir.path().join("key.pem");
    std::fs::write(&cert, key.cert.pem())?;
    std::fs::write(&private_key, key.key_pair.serialize_pem())?;
    let tls = prost_config::ServerTls {
        certificate: cert.clone(),
        private_key,
        ..Default::default()
    };
    let (port, _, task) = server(Some(prost_transport::server_tls(&tls, true)?)).await?;
    let target = format!("https://localhost:{port}");
    let default = client(prost_transport::client_tls(None)?)?;
    assert!(download(&default, &format!("{target}/ok"), LIMIT)
        .await
        .is_err());
    let config = prost_config::ClientTls {
        policy: None,
        server_name: "localhost".into(),
        ca_certificate: Some(cert),
        identity: None,
    };
    let trusted = client(prost_transport::client_tls(Some(&config))?)?;
    assert_eq!(
        download(&trusted, &format!("{target}/ok"), LIMIT).await?,
        b"PEM-data"
    );
    assert!(
        download(&trusted, &format!("https://127.0.0.1:{port}/ok"), LIMIT)
            .await
            .is_err()
    );
    assert!(download(&trusted, &format!("{target}/downgrade"), LIMIT)
        .await
        .unwrap_err()
        .to_string()
        .contains("downgrade"));
    task.abort();
    Ok(())
}
