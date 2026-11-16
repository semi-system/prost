//! One dispatcher per UDP listener, bounded peer queues and connected backend
//! sockets. Listener clones share the peer table across rolling generations.
use super::*;
use std::{collections::HashMap, net::IpAddr, sync::atomic::AtomicU64};
use tokio::{net::UdpSocket, sync::mpsc, time::Instant};

#[derive(Default)]
pub(crate) struct Sessions {
    peers: Mutex<HashMap<SocketAddr, Peer>>,
    next: AtomicU64,
}
struct Peer {
    id: u64,
    tx: mpsc::Sender<Packet>,
    requests: u64,
    limit: u64,
}
struct Packet {
    data: Vec<u8>,
    _memory: OwnedSemaphorePermit,
}
struct Flow {
    peer: SocketAddr,
    id: u64,
    sessions: Arc<Sessions>,
    frontend: Arc<UdpSocket>,
    input: mpsc::Receiver<Packet>,
    listener: Arc<PreparedListener>,
    _snapshot: Arc<Snapshot>,
    _lease: ConnectionLease,
    _memory: OwnedSemaphorePermit,
    closed_drops: prometheus::IntCounter,
    bytes: u64,
}
impl Sessions {
    fn remove(&self, peer: SocketAddr, id: u64) {
        let mut peers = self.peers.lock().unwrap();
        if peers.get(&peer).is_some_and(|entry| entry.id == id) {
            peers.remove(&peer);
        }
    }
}
impl Drop for Flow {
    fn drop(&mut self) {
        self.sessions.remove(self.peer, self.id);
        self.closed_drops.inc_by(self.input.len() as u64);
    }
}
impl RunningProxy {
    pub(crate) fn udp_status(&self) -> serde_json::Value {
        let bindings = self.bindings.lock().unwrap();
        let mappings: BTreeMap<_, _> = bindings
            .iter()
            .filter_map(|(id, binding)| match &binding.masters {
                workers::Masters::Udp(_, sessions) => {
                    Some((id, sessions.peers.lock().unwrap().len()))
                }
                _ => None,
            })
            .collect();
        let maximum = self.snapshot().config.runtime.max_udp_buffer_bytes;
        serde_json::json!({"buffer_budget_bytes":maximum,"buffer_reserved_bytes":maximum.saturating_sub(self.udp_buffers.available_permits()),"peer_mappings":mappings})
    }
    pub(crate) fn start_udp(
        self: &Arc<Self>,
        id: String,
        socket: UdpSocket,
        sessions: Arc<Sessions>,
        generation: Arc<rolling::Generation>,
        cancel: CancellationToken,
    ) {
        let weak = Arc::downgrade(self);
        let accepts = generation.accepts.clone();
        self.tasks.spawn(accepts.track_future(async move {
            let frontend=Arc::new(socket);
            // One fixed receive buffer per listener dispatcher. Responses and
            // every queued packet use the process-wide byte semaphore.
            let mut buffer=vec![0u8;65536];
            loop {
                let packet=tokio::select! {biased;_=cancel.cancelled()=>break,_=generation.accepting.cancelled()=>break,r=frontend.recv_from(&mut buffer)=>r};
                let Some(p)=weak.upgrade() else {break};
                let (size,peer)=match packet {Ok(packet)=>packet,Err(error)=>{p.metrics.udp_dropped.with_label_values(&["io"]).inc();tracing::debug!(%error,"UDP receive failed");continue;}};
                p.metrics.udp_datagrams.with_label_values(&["client"]).inc();
                let snapshot=if generation.rolling {generation.snapshot.load_full().expect("UDP accept snapshot retained until accepts stop")}else{p.snapshot()};
                let Some(listener)=snapshot.listeners.get(&id).cloned() else{continue};
                let settings=listener.config.udp.clone().unwrap_or_default();
                if size>settings.max_datagram_bytes {p.metrics.udp_dropped.with_label_values(&["oversize"]).inc();continue;}
                let memory=match p.udp_buffers.clone().try_acquire_many_owned((size+64) as u32) {Ok(memory)=>memory,Err(_)=>{p.metrics.udp_dropped.with_label_values(&["buffer_capacity"]).inc();continue;}};
                let packet=Packet {data:buffer[..size].to_vec(),_memory:memory};
                let mut peers=sessions.peers.lock().unwrap();
                if let Some(existing)=peers.get_mut(&peer) {
                    let mut completed=false;
                    if existing.tx.try_send(packet).is_err() {p.metrics.udp_dropped.with_label_values(&["queue_capacity"]).inc();}
                    else {existing.requests=existing.requests.saturating_add(1);completed=existing.limit!=0 && existing.requests>=existing.limit;}
                    if completed {peers.remove(&peer);}
                    continue;
                }
                let admitted=(||->Result<_> {
                    let global=p.connections.clone().try_acquire_owned()?;
                    let local=listener.capacity.clone().try_acquire_owned()?;
                    let ip=listener.ip_capacity.acquire(peer.ip(),listener.config.max_connections_per_ip)?;
                    let response=p.udp_buffers.clone().try_acquire_many_owned((settings.max_datagram_bytes+1) as u32)?;
                    Ok((global,local,ip,response))
                })();
                let (global,local,ip,response)=match admitted {Ok(permits)=>permits,Err(_)=>{p.metrics.udp_dropped.with_label_values(&["session_capacity"]).inc();continue;}};
                let (tx,input)=mpsc::channel(settings.session_queue_packets);
                if tx.try_send(packet).is_err() {continue;}
                let session_id=sessions.next.fetch_add(1,Ordering::Relaxed);
                if settings.session_requests!=1 {peers.insert(peer,Peer {id:session_id,tx,requests:1,limit:settings.session_requests});}
                drop(peers);
                let audit=if listener.config.access_log {p.access_log.as_ref().filter(|log|log.sample()).map(|log|access_log::Ticket::udp(log.clone(),&peer.to_string(),&id,snapshot.config.generation))}else{None};
                p.metrics.connections.inc();generation.connections.fetch_add(1,Ordering::Relaxed);
                let lease=ConnectionLease {generation:generation.clone(),_ip:Some(ip),audit:audit.clone(),_global:global,_local:local,_plugins:Vec::new(),active:p.metrics.connections.clone()};
                let mut flow=Flow {peer,id:session_id,sessions:sessions.clone(),frontend:frontend.clone(),input,listener,_snapshot:snapshot,_lease:lease,_memory:response,closed_drops:p.metrics.udp_dropped.with_label_values(&["session_closed"]),bytes:0};
                let worker_force=generation.force.clone();let task_proxy=p.clone();
                p.tasks.spawn(generation.tasks.track_future(async move {
                    let (result,aborted)=tokio::select! {biased;_=task_proxy.force.cancelled()=>(Ok(()),true),_=worker_force.cancelled()=>(Ok(()),true),r=flow.run(&task_proxy)=>(r,false)};
                    task_proxy.metrics.bytes.inc_by(flow.bytes);
                    if let Some(audit)=audit {audit.finish(Some(flow.bytes),if aborted {"aborted"}else if result.is_err(){"error"}else{"complete"});}
                    if let Err(error)=result {task_proxy.metrics.errors.inc();task_proxy.metrics.udp_dropped.with_label_values(&["io"]).inc();tracing::debug!(%error,%peer,"UDP session ended with error");}
                }));
            }
        }));
    }
}
impl Flow {
    async fn run(&mut self, p: &RunningProxy) -> Result<()> {
        let settings = self.listener.config.udp.clone().unwrap_or_default();
        let u = self
            .listener
            .default_upstream
            .as_ref()
            .context("UDP upstream missing")?;
        let selection = u
            .balancer
            .select_tcp(u, self.peer.ip())
            .map_err(|error| u.selection_error(error))?;
        let index = selection.index;
        let _lease = selection.lease;
        u.observed[index].selected();
        if let Some(audit) = &self._lease.audit {
            audit.upstream(
                self.listener.config.upstream.as_deref(),
                Some(&u.config.endpoints[index].address),
            );
        }
        let backend =
            tokio::time::timeout(Duration::from_millis(u.config.connect_timeout_ms), async {
                let address = &u.config.endpoints[index].address;
                let addresses = if let Some(dns) = &u.dns {
                    dns.endpoint(address).await?
                } else if let Ok(address) = address.parse::<SocketAddr>() {
                    vec![address]
                } else {
                    let _permit = p
                        .health_capacity
                        .try_acquire()
                        .context("UDP DNS capacity")?;
                    tokio::net::lookup_host(address)
                        .await?
                        .take(32)
                        .collect::<Vec<_>>()
                };
                let mut last = None;
                for address in addresses {
                    let local = match address.ip() {
                        IpAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
                        IpAddr::V6(_) => SocketAddr::from(([0u16; 8], 0)),
                    };
                    match UdpSocket::bind(local).await {
                        Ok(socket) => match socket.connect(address).await {
                            Ok(()) => return Ok(socket),
                            Err(error) => last = Some(error),
                        },
                        Err(error) => last = Some(error),
                    }
                }
                Err(anyhow::Error::from(last.unwrap_or_else(|| {
                    std::io::Error::other("UDP DNS returned no addresses")
                })))
            })
            .await
            .context("UDP resolve/connect timeout")??;
        let mut buffer = vec![0u8; settings.max_datagram_bytes + 1];
        let idle = Duration::from_millis(settings.idle_timeout_ms);
        let mut deadline = Instant::now() + idle;
        let mut requests = 0u64;
        let mut responses = 0u64;
        loop {
            tokio::select! {
                _=tokio::time::sleep_until(deadline)=>return Ok(()),
                packet=self.input.recv(),if settings.session_requests==0 || requests<settings.session_requests=>{
                    let Some(packet)=packet else{return Ok(())};
                    tokio::time::timeout_at(deadline,backend.send(&packet.data)).await.context("UDP backend send timeout")??;
                    self.bytes+=packet.data.len() as u64;requests=requests.saturating_add(1);deadline=Instant::now()+idle;
                    if settings.session_requests!=0 && requests>=settings.session_requests {self.sessions.remove(self.peer,self.id);}
                },
                result=backend.recv(&mut buffer)=>{
                    let size=result?;
                    p.metrics.udp_datagrams.with_label_values(&["backend"]).inc();
                    if size>settings.max_datagram_bytes {p.metrics.udp_dropped.with_label_values(&["oversize"]).inc();continue;}
                    tokio::time::timeout_at(deadline,self.frontend.send_to(&buffer[..size],self.peer)).await.context("UDP client send timeout")??;
                    self.bytes+=size as u64;responses=responses.saturating_add(1);deadline=Instant::now()+idle;
                    if settings.session_responses!=0 && responses>=settings.session_responses {return Ok(())}
                }
            }
        }
    }
}
