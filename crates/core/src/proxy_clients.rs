use crate::{ClientPool, PreparedUpstream};
use anyhow::Result;
use prost_http::HttpClient;
use prost_transport::ProxyAddresses;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};

type Entry = (Weak<ClientPool>, u64, HttpClient);
/// A backend connection belongs to one frontend TCP connection. Never share a
/// PROXY-prefixed transport between clients, including multiplexed HTTP/2.
pub(crate) struct Clients {
    clients: Mutex<BTreeMap<String, Entry>>,
    capacity: Arc<tokio::sync::Semaphore>,
}
impl Clients {
    pub fn new(capacity: Arc<tokio::sync::Semaphore>) -> Self {
        Self {
            clients: Default::default(),
            capacity,
        }
    }
    pub fn get(
        &self,
        upstream: &Arc<PreparedUpstream>,
        name: &str,
        addresses: ProxyAddresses,
    ) -> Result<HttpClient> {
        let mut clients = self.clients.lock().unwrap();
        let revision = upstream.dns.as_ref().map_or(0, |dns| dns.revision());
        if let Some((identity, generation, client)) = clients.get(name) {
            if identity
                .upgrade()
                .is_some_and(|old| Arc::ptr_eq(&old, &upstream.client))
                && *generation == revision
            {
                return Ok(client.clone());
            }
        }
        clients.retain(|_, (identity, _, _)| identity.strong_count() > 0);
        // Bound long-lived frontend connections that visit many generations.
        if clients.len() >= 128 && !clients.contains_key(name) {
            clients.pop_first();
        }
        let client = prost_http::client_with_proxy_dns(
            &upstream.config,
            upstream.tls.clone(),
            upstream.observed_group.pool.clone(),
            prost_http::worker_slot(),
            addresses,
            self.capacity.clone(),
            upstream.dns.clone(),
        )?;
        clients.insert(
            name.to_owned(),
            (Arc::downgrade(&upstream.client), revision, client.clone()),
        );
        Ok(client)
    }
}
