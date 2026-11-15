use anyhow::{ensure, Context, Result};
use http_body_util::BodyExt;
use std::{path::Path, time::Duration};
struct Driver(tokio::task::AbortHandle);
impl Drop for Driver {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub async fn post(
    url: &str,
    tls: Option<&prost_config::ClientTls>,
    bearer: Option<&Path>,
    body: serde_json::Value,
    id: &str,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let uri = url::Url::parse(url)?;
        ensure!(uri.scheme() == "https", "webhook requires HTTPS");
        let host = uri.host_str().context("webhook host")?;
        let tcp =
            tokio::net::TcpStream::connect((host, uri.port_or_known_default().unwrap_or(443)))
                .await?;
        let mut config = tls.cloned().unwrap_or(prost_config::ClientTls {
            server_name: host.into(),
            ca_certificate: None,
            identity: None,
            policy: None,
        });
        config.server_name = host.into();
        let io = tokio_rustls::TlsConnector::from(prost_transport::client_tls(Some(&config))?)
            .connect(
                rustls::pki_types::ServerName::try_from(host.to_owned())?,
                tcp,
            )
            .await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let _driver = Driver(driver.abort_handle());
        let result = async {
            let mut request = ::http::Request::builder()
                .method("POST")
                .uri(match uri.query() {
                    Some(query) => format!("{}?{query}", uri.path()),
                    None => uri.path().to_owned(),
                })
                .header("host", uri.authority())
                .header("content-type", "application/json")
                .header("idempotency-key", id);
            if let Some(path) = bearer {
                let token = String::from_utf8(crate::store::private_read(path, 16384)?)?;
                request = request.header("authorization", format!("Bearer {}", token.trim()));
            }
            let response =
                sender
                    .send_request(request.body(http_body_util::Full::new(
                        hyper::body::Bytes::from(serde_json::to_vec(&body)?),
                    ))?)
                    .await?;
            ensure!(response.status().is_success(), "webhook rejected request");
            http_body_util::Limited::new(response.into_body(), 4096)
                .collect()
                .await
                .map_err(anyhow::Error::from_boxed)?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        driver.abort();
        result
    })
    .await?
}
