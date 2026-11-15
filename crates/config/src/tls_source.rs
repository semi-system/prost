use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use url::Url;
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct S3Config {
    pub ca_certificate: Option<PathBuf>,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub force_path_style: bool,
}
impl S3Config {
    pub fn validate(&self) -> Result<()> {
        if let Some(ca) = &self.ca_certificate {
            ensure!(
                matches!(TlsSource::parse(ca)?, TlsSource::File(_)),
                "S3 CA must be a local path or file URI"
            );
        }
        if let Some(region) = &self.region {
            ensure!(
                !region.is_empty()
                    && region.len() <= 64
                    && region
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
                "invalid S3 region"
            );
        }
        if let Some(endpoint) = &self.endpoint {
            let url = Url::parse(endpoint).context("invalid S3 endpoint")?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/",
                "S3 endpoint must be an HTTP(S) origin without credentials/query/path"
            );
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct S3Object {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum TlsSource {
    File(PathBuf),
    Remote(Url),
    S3(S3Object),
}
impl TlsSource {
    pub fn parse(path: &Path) -> Result<Self> {
        ensure!(
            !path.as_os_str().is_empty(),
            "TLS artifact source is required"
        );
        let Some(text) = path.to_str().filter(|s| {
            !path.is_absolute()
                && s.split_once("://").is_some_and(|(scheme, _)| {
                    scheme
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphabetic)
                        && scheme
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
                })
        }) else {
            return Ok(Self::File(path.to_owned()));
        };
        ensure!(
            text.len() <= 8192 && !text.chars().any(char::is_control),
            "invalid TLS artifact URI length/characters"
        );
        if let Some((_, rest)) = text
            .split_once("://")
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("s3"))
        {
            ensure!(!rest.contains('#'), "S3 URI must not contain a fragment");
            let (bucket, path) = rest.split_once('/').context("S3 URI requires bucket/key")?;
            ensure!(
                (3..=63).contains(&bucket.len())
                    && bucket
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && bucket
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && bucket.bytes().all(|b| b.is_ascii_lowercase()
                        || b.is_ascii_digit()
                        || matches!(b, b'.' | b'-'))
                    && !bucket.contains("..")
                    && bucket.parse::<std::net::Ipv4Addr>().is_err(),
                "invalid S3 bucket name"
            );
            let (key, query) = path
                .split_once('?')
                .map_or((path, None), |(k, q)| (k, Some(q)));
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8()
                .context("S3 key must be UTF-8")?
                .into_owned();
            ensure!(
                !key.is_empty() && key.len() <= 1024 && !key.chars().any(char::is_control),
                "invalid S3 object key"
            );
            let mut version_id = None;
            if let Some(query) = query {
                let pairs: Vec<_> = url::form_urlencoded::parse(query.as_bytes()).collect();
                ensure!(
                    pairs.len() == 1 && pairs[0].0 == "versionId" && !pairs[0].1.is_empty(),
                    "S3 URI query only supports one versionId"
                );
                version_id = Some(pairs[0].1.to_string());
            }
            return Ok(Self::S3(S3Object {
                bucket: bucket.into(),
                key,
                version_id,
            }));
        }
        let uri = Url::parse(text).context("invalid TLS artifact URI")?;
        ensure!(
            uri.username().is_empty() && uri.password().is_none() && uri.fragment().is_none(),
            "TLS artifact URI must not contain credentials or a fragment"
        );
        match uri.scheme() {
            "file" => {
                ensure!(
                    uri.query().is_none() && uri.host_str().is_none_or(|h| h == "localhost"),
                    "file URI must reference a local file without a query"
                );
                let path = uri
                    .to_file_path()
                    .map_err(|_| anyhow::anyhow!("invalid local file URI"))?;
                ensure!(path.is_absolute(), "file URI must use an absolute path");
                Ok(Self::File(path))
            }
            "http" | "https" => {
                ensure!(uri.host_str().is_some(), "TLS artifact URI requires a host");
                Ok(Self::Remote(uri))
            }
            _ => bail!("unsupported TLS artifact URI scheme; use file/http/https/s3"),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn s3_uris_preserve_key_bytes_and_validate_settings() -> Result<()> {
        let TlsSource::S3(object) = TlsSource::parse(Path::new(
            "s3://cert-bucket/dir/../a%20b+%25//cert.pem?versionId=v%2Fa%2Bb%3D",
        ))?
        else {
            panic!("expected S3");
        };
        assert_eq!(object.bucket, "cert-bucket");
        assert_eq!(object.key, "dir/../a b+%//cert.pem");
        assert_eq!(object.version_id.as_deref(), Some("v/a+b="));
        for uri in [
            "s3://UPPER/key",
            "s3://127.0.0.1/key",
            "s3://certs/",
            "s3://certs/key?other=x",
            "s3://certs/key#fragment",
            "s3://certs/key?versionId=x&versionId=y",
        ] {
            assert!(TlsSource::parse(Path::new(uri)).is_err(), "{uri}");
        }
        for endpoint in [
            "ftp://example.com",
            "https://user:secret@example.com",
            "https://example.com/prefix",
            "https://example.com?token=x",
        ] {
            assert!(S3Config {
                endpoint: Some(endpoint.into()),
                ..Default::default()
            }
            .validate()
            .is_err());
        }
        assert!(S3Config {
            ca_certificate: Some("https://example.com/ca.pem".into()),
            ..Default::default()
        }
        .validate()
        .is_err());
        Ok(())
    }
    #[test]
    fn local_paths_file_uris_and_http_sources_validate_without_io() -> Result<()> {
        assert!(
            matches!(TlsSource::parse(Path::new("cert.pem"))?,TlsSource::File(p) if p==Path::new("cert.pem"))
        );
        assert!(
            matches!(TlsSource::parse(Path::new("file:///tmp/full%20chain.pem"))?,TlsSource::File(p) if p==Path::new("/tmp/full chain.pem"))
        );
        assert!(matches!(
            TlsSource::parse(Path::new("file://localhost/tmp/cert.pem"))?,
            TlsSource::File(_)
        ));
        for source in [
            "http://127.0.0.1:8080/fullchain.pem",
            "https://cert.example/key.pem?token=opaque",
        ] {
            assert!(matches!(
                TlsSource::parse(Path::new(source))?,
                TlsSource::Remote(_)
            ));
        }
        for source in [
            "",
            "file://elsewhere/tmp/cert.pem",
            "file:///tmp/cert.pem?x=1",
            "https://user:secret@cert.example/key.pem",
            "https://cert.example/file#fragment",
            "ftp://cert.example/file",
            "https://[bad]/file",
        ] {
            assert!(TlsSource::parse(Path::new(source)).is_err());
        }
        Ok(())
    }
}
