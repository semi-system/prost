use anyhow::{ensure, Result};
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
};

/// Entries exist only while connections hold permits, bounded by global admission.
#[derive(Default)]
pub(crate) struct IpConnections(Mutex<HashMap<IpAddr, usize>>);
pub(crate) struct Permit {
    owner: Arc<IpConnections>,
    ip: IpAddr,
}
impl IpConnections {
    pub fn acquire(self: &Arc<Self>, ip: IpAddr, limit: usize) -> Result<Permit> {
        let ip = match ip {
            IpAddr::V6(ip) => ip
                .to_ipv4_mapped()
                .map(IpAddr::V4)
                .unwrap_or(IpAddr::V6(ip)),
            ip => ip,
        };
        let mut counts = self.0.lock().unwrap();
        let count = counts.get(&ip).copied().unwrap_or(0);
        ensure!(limit == 0 || count < limit, "per-IP connection capacity");
        counts.insert(ip, count + 1);
        Ok(Permit {
            owner: self.clone(),
            ip,
        })
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut counts = self.owner.0.lock().unwrap();
        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.ip);
            }
        }
    }
}
