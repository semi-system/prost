use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Http3Settings {
    pub max_connections: usize,
    pub max_streams: u32,
    pub connection_receive_window: u32,
    pub stream_receive_window: u32,
    pub send_window: u32,
    pub idle_timeout_ms: u64,
    pub stream_write_timeout_ms: u64,
}
impl Default for Http3Settings {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_streams: 32,
            connection_receive_window: 1024 * 1024,
            stream_receive_window: 65536,
            send_window: 1024 * 1024,
            idle_timeout_ms: 60_000,
            stream_write_timeout_ms: 30_000,
        }
    }
}
impl Http3Settings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=10_000).contains(&self.max_connections),
            "HTTP/3 max_connections must be 1..10000"
        );
        ensure!(
            (1..=1024).contains(&self.max_streams),
            "HTTP/3 max_streams must be 1..1024"
        );
        for size in [
            self.connection_receive_window,
            self.stream_receive_window,
            self.send_window,
        ] {
            ensure!(
                (16384..=64 * 1024 * 1024).contains(&size),
                "HTTP/3 windows must be 16KiB..64MiB"
            );
        }
        ensure!(
            self.stream_receive_window <= self.connection_receive_window,
            "HTTP/3 stream window exceeds connection window"
        );
        ensure!(
            (1..=86_400_000).contains(&self.idle_timeout_ms),
            "HTTP/3 idle timeout must be 1..86400000 ms"
        );
        ensure!(
            self.stream_write_timeout_ms <= 86_400_000,
            "invalid HTTP/3 write timeout"
        );
        Ok(())
    }
}
