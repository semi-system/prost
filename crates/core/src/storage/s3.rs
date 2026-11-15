use super::{read, FetchClient};
use anyhow::{ensure, Context, Result};
use aws_credential_types::provider::ProvideCredentials;
use aws_sigv4::{
    http_request::{
        sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest,
        SigningSettings, UriPathNormalizationMode,
    },
    sign::v4,
};
use http::{header, Request};
use http_body_util::Empty;
use prost_config::{S3Config, S3Object};
use std::time::SystemTime;

pub(super) struct S3 {
    client: FetchClient,
    config: aws_types::SdkConfig,
    settings: S3Config,
}
impl S3 {
    pub async fn new(settings: &S3Config) -> Result<Self> {
        settings.validate()?;
        let http = aws_smithy_http_client::Builder::new()
            .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
            ))
            .build_https();
        let mut loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).http_client(http);
        if let Some(region) = &settings.region {
            loader = loader.region(aws_types::region::Region::new(region.clone()));
        }
        let config = loader.load().await;
        ensure!(
            config.region().is_some(),
            "S3 region is required (runtime.s3.region or AWS_REGION)"
        );
        Ok(Self {
            client: super::client(prost_transport::client_tls(
                settings
                    .ca_certificate
                    .as_ref()
                    .map(|ca| prost_config::ClientTls {
                        policy: None,
                        server_name: "source".into(),
                        ca_certificate: Some(ca.clone()),
                        identity: None,
                    })
                    .as_ref(),
            )?)?,
            config,
            settings: settings.clone(),
        })
    }
    pub async fn get(&self, object: S3Object, limit: usize) -> Result<Vec<u8>> {
        let region = self.config.region().context("missing S3 region")?.as_ref();
        let credentials = self
            .config
            .credentials_provider()
            .context("no AWS credential provider")?
            .provide_credentials()
            .await
            .map_err(|_| anyhow::anyhow!("AWS credentials unavailable"))?;
        let uri = object_uri(&self.settings, region, &object)?;
        let mut request = Request::builder()
            .uri(&uri)
            .header(header::ACCEPT_ENCODING, "identity")
            .body(Empty::new())?;
        let identity = credentials.into();
        let mut settings = SigningSettings::default();
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region(region)
            .name("s3")
            .time(SystemTime::now())
            .settings(settings)
            .build()
            .context("S3 signing configuration failed")?
            .into();
        let signing = SignableRequest::new(
            "GET",
            &uri,
            request
                .headers()
                .iter()
                .map(|(n, v)| Ok((n.as_str(), v.to_str()?)))
                .collect::<Result<Vec<_>>>()?
                .into_iter(),
            SignableBody::Bytes(&[]),
        )
        .context("S3 request signing failed")?;
        let (instructions, _) = sign(signing, &params)
            .context("S3 request signing failed")?
            .into_parts();
        instructions.apply_to_request_http1x(&mut request);
        // Signed requests never follow redirects or forward AWS authorization
        // to a different endpoint. Configure the correct bucket region.
        let response = self
            .client
            .request(request)
            .await
            .map_err(|_| anyhow::anyhow!("S3 artifact request failed"))?;
        read(response, limit).await
    }
}
fn encode(text: &str, slash: bool) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') || slash && b == b'/'
        {
            out.push(char::from(b));
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
fn object_uri(settings: &S3Config, region: &str, object: &S3Object) -> Result<String> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid S3 region"
    );
    let suffix = if region.starts_with("cn-") {
        "amazonaws.com.cn"
    } else {
        "amazonaws.com"
    };
    let path_style = settings.force_path_style || object.bucket.contains('.');
    let origin = if let Some(endpoint) = &settings.endpoint {
        let mut endpoint = url_origin(endpoint)?;
        if !path_style {
            endpoint = format!(
                "{}://{}.{}",
                endpoint.split_once("://").unwrap().0,
                object.bucket,
                endpoint.split_once("://").unwrap().1
            );
        }
        endpoint
    } else if path_style {
        format!("https://s3.{region}.{suffix}")
    } else {
        format!("https://{}.s3.{region}.{suffix}", object.bucket)
    };
    let prefix = if path_style {
        format!("/{}/", object.bucket)
    } else {
        "/".into()
    };
    let mut uri = format!("{origin}{prefix}{}", encode(&object.key, true));
    if let Some(version) = &object.version_id {
        uri.push_str("?versionId=");
        uri.push_str(&encode(version, false));
    }
    Ok(uri)
}
fn url_origin(endpoint: &str) -> Result<String> {
    S3Config {
        endpoint: Some(endpoint.into()),
        ..Default::default()
    }
    .validate()?;
    Ok(endpoint.trim_end_matches('/').into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn s3_endpoints_keys_and_versions_preserve_object_identity() -> Result<()> {
        let object = S3Object {
            bucket: "test-bucket".into(),
            key: "dir/../a b+%//cert.pem".into(),
            version_id: Some("version/a+b=".into()),
        };
        assert_eq!(object_uri(&S3Config::default(),"us-east-1",&object)?,"https://test-bucket.s3.us-east-1.amazonaws.com/dir/../a%20b%2B%25//cert.pem?versionId=version%2Fa%2Bb%3D");
        let settings = S3Config {
            endpoint: Some("http://127.0.0.1:9000/".into()),
            force_path_style: true,
            ..Default::default()
        };
        assert!(object_uri(&settings, "us-east-1", &object)?
            .starts_with("http://127.0.0.1:9000/test-bucket/"));
        assert!(
            object_uri(&S3Config::default(), "cn-north-1", &object)?.contains("amazonaws.com.cn")
        );
        Ok(())
    }
}
