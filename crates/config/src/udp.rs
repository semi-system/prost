use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UdpSettings {
    pub idle_timeout_ms: u64,
    pub max_datagram_bytes: usize,
    pub session_queue_packets: usize,
    /// Zero retains the client mapping until idle timeout.
    pub session_requests: u64,
    /// Zero permits any number of replies before idle timeout.
    pub session_responses: u64,
}
impl Default for UdpSettings {
    fn default() -> Self {
        Self {
            idle_timeout_ms: 30_000,
            max_datagram_bytes: 65507,
            session_queue_packets: 8,
            session_requests: 0,
            session_responses: 0,
        }
    }
}
impl UdpSettings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=86_400_000).contains(&self.idle_timeout_ms),
            "UDP idle timeout must be 1..86400000 ms"
        );
        ensure!(
            (1..=65507).contains(&self.max_datagram_bytes),
            "UDP max_datagram_bytes must be 1..65507"
        );
        ensure!(
            (1..=1024).contains(&self.session_queue_packets),
            "UDP session queue must be 1..1024 packets"
        );
        Ok(())
    }
}
