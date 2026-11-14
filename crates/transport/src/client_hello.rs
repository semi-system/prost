use crate::BoxStream;
use anyhow::{ensure, Context, Result};
use prost_config::{PassthroughSettings, ServerTls};
use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    pin::Pin,
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

pub struct ClientHelloGuard {
    config: PassthroughSettings,
    exact: HashSet<String>,
    wildcard: HashSet<String>,
    routes_exact: HashMap<String, String>,
    routes_wildcard: HashMap<String, String>,
}
/// The stream replays every byte read during inspection, including any bytes
/// following ClientHello. TLS remains end-to-end between client and backend.
pub struct InspectedHello {
    pub stream: BoxStream,
    pub upstream: Option<String>,
}
impl ClientHelloGuard {
    pub fn new(config: PassthroughSettings) -> Result<Self> {
        config.validate()?;
        let mut exact = HashSet::new();
        let mut wildcard = HashSet::new();
        let mut routes_exact = HashMap::new();
        let mut routes_wildcard = HashMap::new();
        for name in &config.server_names {
            let name = ServerTls::normalize_name(name)?;
            if let Some(suffix) = name.strip_prefix("*.") {
                wildcard.insert(suffix.to_owned());
            } else {
                exact.insert(name);
            }
        }
        for route in &config.routes {
            for name in &route.server_names {
                let name = ServerTls::normalize_name(name)?;
                if let Some(suffix) = name.strip_prefix("*.") {
                    wildcard.insert(suffix.to_owned());
                    routes_wildcard.insert(suffix.to_owned(), route.upstream.clone());
                } else {
                    exact.insert(name.clone());
                    routes_exact.insert(name, route.upstream.clone());
                }
            }
        }
        Ok(Self {
            config,
            exact,
            wildcard,
            routes_exact,
            routes_wildcard,
        })
    }
    fn allowed(&self, name: Option<&str>) -> bool {
        if !self.config.reject_unknown_sni {
            return true;
        }
        name.is_some_and(|name| {
            let name = name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase();
            self.exact.contains(&name)
                || name
                    .split_once('.')
                    .is_some_and(|(_, suffix)| self.wildcard.contains(suffix))
        })
    }
    fn upstream(&self, name: Option<&str>) -> Option<String> {
        let name = name?;
        let name = name.strip_suffix('.').unwrap_or(name).to_ascii_lowercase();
        self.routes_exact
            .get(&name)
            .or_else(|| {
                name.split_once('.')
                    .and_then(|(_, suffix)| self.routes_wildcard.get(suffix))
            })
            .cloned()
    }
    pub async fn inspect(&self, io: BoxStream) -> Result<BoxStream> {
        Ok(self.inspect_routed(io).await?.stream)
    }
    pub async fn inspect_routed(&self, mut io: BoxStream) -> Result<InspectedHello> {
        tokio::time::timeout(
            Duration::from_millis(self.config.client_hello_timeout_ms),
            async {
                let mut acceptor = rustls::server::Acceptor::default();
                let mut prefix = Vec::with_capacity(4096);
                let mut buffer = [0u8; 4096];
                loop {
                    let available = self
                        .config
                        .max_client_hello_bytes
                        .saturating_sub(prefix.len());
                    ensure!(available > 0, "passthrough ClientHello exceeds byte budget");
                    let n = io.read(&mut buffer[..available.min(4096)]).await?;
                    ensure!(n > 0, "EOF before passthrough ClientHello");
                    prefix.extend_from_slice(&buffer[..n]);
                    let mut input = Cursor::new(&buffer[..n]);
                    while input.position() < n as u64 {
                        ensure!(
                            acceptor.read_tls(&mut input)? > 0,
                            "ClientHello parser made no progress"
                        );
                        match acceptor.accept() {
                            Ok(Some(accepted)) => {
                                let hello = accepted.client_hello();
                                ensure!(
                                    self.allowed(hello.server_name()),
                                    "unrecognized passthrough SNI"
                                );
                                return Ok(InspectedHello {
                                    upstream: self.upstream(hello.server_name()),
                                    stream: Box::new(Replay {
                                        prefix: Cursor::new(prefix),
                                        io,
                                    }),
                                });
                            }
                            Ok(None) => {}
                            Err((error, _)) => return Err(error.into()),
                        }
                    }
                }
            },
        )
        .await
        .context("passthrough ClientHello timeout")?
    }
}
struct Replay {
    prefix: Cursor<Vec<u8>>,
    io: BoxStream,
}
impl AsyncRead for Replay {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.prefix.position() < self.prefix.get_ref().len() as u64 {
            Pin::new(&mut self.prefix).poll_read(cx, buf)
        } else {
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }
}
impl AsyncWrite for Replay {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write_vectored(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_config::PassthroughRoute;
    use tokio::io::AsyncWriteExt;
    fn settings() -> PassthroughSettings {
        PassthroughSettings {
            reject_unknown_sni: true,
            server_names: vec!["fallback.other.test".into()],
            routes: vec![
                PassthroughRoute {
                    server_names: vec!["*.example.test".into()],
                    upstream: "wild".into(),
                },
                PassthroughRoute {
                    server_names: vec!["api.example.test".into()],
                    upstream: "exact".into(),
                },
            ],
            ..Default::default()
        }
    }
    #[test]
    fn exact_routes_override_single_label_wildcard_and_allow_only_names_fall_back() -> Result<()> {
        let guard = ClientHelloGuard::new(settings())?;
        assert_eq!(
            guard.upstream(Some("API.Example.TEST.")).as_deref(),
            Some("exact")
        );
        assert_eq!(
            guard.upstream(Some("x.example.test")).as_deref(),
            Some("wild")
        );
        for name in [
            None,
            Some("example.test"),
            Some("deep.x.example.test"),
            Some("unknown.test"),
        ] {
            assert!(!guard.allowed(name));
            assert!(guard.upstream(name).is_none());
        }
        assert!(guard.allowed(Some("FALLBACK.OTHER.TEST.")));
        assert!(guard.upstream(Some("fallback.other.test")).is_none());
        Ok(())
    }
    #[tokio::test]
    async fn fragmented_clienthello_and_following_bytes_are_replayed_exactly_once() -> Result<()> {
        let guard = ClientHelloGuard::new(settings())?;
        let mut client = rustls::ClientConnection::new(
            crate::client_tls(None)?,
            "api.example.test".try_into()?,
        )?;
        let mut bytes = Vec::new();
        client.write_tls(&mut bytes)?;
        bytes.extend_from_slice(b"\x17\x03\x03\x00\x05extra");
        let expected = bytes.clone();
        let (mut sender, receiver) = tokio::io::duplex(8192);
        let producer = tokio::spawn(async move {
            for chunk in bytes.chunks(3) {
                sender.write_all(chunk).await?;
                tokio::task::yield_now().await;
            }
            sender.shutdown().await
        });
        let mut inspected = guard.inspect_routed(Box::new(receiver)).await?;
        assert_eq!(inspected.upstream.as_deref(), Some("exact"));
        let mut replayed = Vec::new();
        inspected.stream.read_to_end(&mut replayed).await?;
        producer.await??;
        assert_eq!(replayed, expected);
        Ok(())
    }
}
