use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    os::fd::AsFd,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Duration,
};
use tokio::{
    net::{TcpListener, UdpSocket},
    runtime::Handle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

/// Track Quinn's endpoint/connection drivers as well as application handlers.
/// Waiting for their completion proves that the UDP socket has been released.
#[derive(Debug)]
struct QuicRuntime(TaskTracker);
impl quinn::Runtime for QuicRuntime {
    fn new_timer(&self, at: std::time::Instant) -> std::pin::Pin<Box<dyn quinn::AsyncTimer>> {
        quinn::Runtime::new_timer(&quinn::TokioRuntime, at)
    }
    fn spawn(&self, future: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>) {
        self.0.spawn(future);
    }
    fn wrap_udp_socket(
        &self,
        socket: std::net::UdpSocket,
    ) -> std::io::Result<Arc<dyn quinn::AsyncUdpSocket>> {
        quinn::Runtime::wrap_udp_socket(&quinn::TokioRuntime, socket)
    }
}

pub(crate) struct Workers {
    handles: Vec<Handle>,
    stop: CancellationToken,
    threads: Mutex<Vec<JoinHandle<()>>>,
}
pub(crate) struct BoundListener {
    pub sockets: Vec<(Handle, TcpListener)>,
    pub masters: Masters,
    pub udp: Option<(UdpSocket, Arc<crate::udp::Sessions>)>,
    pub quic: Option<quinn::Endpoint>,
}
pub(crate) enum Masters {
    Tcp(Vec<std::net::TcpListener>),
    Udp(std::net::UdpSocket, Arc<crate::udp::Sessions>),
    Quic(quinn::Endpoint),
}
impl BoundListener {
    fn new(sockets: Vec<(Handle, TcpListener)>) -> Result<Self> {
        let masters = sockets
            .iter()
            .map(|(_, s)| Ok(std::net::TcpListener::from(s.as_fd().try_clone_to_owned()?)))
            .collect::<std::io::Result<Vec<_>>>()?;
        Ok(Self {
            sockets,
            masters: Masters::Tcp(masters),
            udp: None,
            quic: None,
        })
    }
}

impl Workers {
    pub fn new(count: usize, latency: prometheus::Histogram) -> Result<Self> {
        let mut workers = Self {
            handles: Vec::with_capacity(count),
            stop: CancellationToken::new(),
            threads: Mutex::new(Vec::with_capacity(count)),
        };
        for index in 0..count {
            let stop = workers.stop.clone();
            let latency = latency.clone();
            let (tx, rx) = std::sync::mpsc::sync_channel(1);
            let thread=std::thread::Builder::new().name(format!("prost-io-{index}")).spawn(move || {
                let runtime=tokio::runtime::Builder::new_current_thread().enable_all().max_blocking_threads(8).build();
                match runtime {
                    Ok(runtime)=>{
                        prost_http::set_worker_slot(index);
                        super::local_metrics::initialize(&latency);
                        let _=tx.send(Ok(runtime.handle().clone()));
                        runtime.block_on(async {
                            let mut tick=tokio::time::interval(Duration::from_millis(250));
                            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {tokio::select!{_=stop.cancelled()=>break,_=tick.tick()=>super::local_metrics::flush()}}
                            super::local_metrics::flush();
                        });
                        runtime.shutdown_timeout(Duration::from_secs(1));
                        super::local_metrics::flush();
                    },
                    Err(error)=>{let _=tx.send(Err(error.to_string()));}
                }
            })?;
            workers.threads.lock().unwrap().push(thread);
            workers.handles.push(
                rx.recv()
                    .context("I/O worker startup failed")?
                    .map_err(anyhow::Error::msg)?,
            );
        }
        Ok(workers)
    }
    pub fn stop(&self) {
        self.stop.cancel();
    }
    pub async fn ready(&self) -> Result<()> {
        for handle in &self.handles {
            tokio::time::timeout(Duration::from_secs(3), handle.spawn(async {}))
                .await
                .context("worker readiness timeout")??;
        }
        Ok(())
    }
    pub fn attach(&self, masters: &Masters) -> Result<BoundListener> {
        let Masters::Tcp(masters) = masters else {
            return attach_udp(masters);
        };
        anyhow::ensure!(
            masters.len() == self.handles.len(),
            "listener worker count mismatch"
        );
        let sockets = self
            .handles
            .iter()
            .zip(masters)
            .map(|(handle, master)| {
                let socket = master.try_clone()?;
                let _guard = handle.enter();
                Ok((handle.clone(), TcpListener::from_std(socket)?))
            })
            .collect::<Result<Vec<_>>>()?;
        BoundListener::new(sockets)
    }
    pub fn join(&self) {
        self.stop();
        for thread in self.threads.lock().unwrap().drain(..) {
            let _ = thread.join();
        }
    }
    pub fn bind(&self, address: SocketAddr) -> Result<BoundListener> {
        let mut sockets = Vec::with_capacity(self.handles.len());
        for handle in &self.handles {
            let socket = socket2::Socket::new(
                socket2::Domain::for_address(address),
                socket2::Type::STREAM,
                Some(socket2::Protocol::TCP),
            )?;
            socket.set_reuse_address(true)?;
            socket.set_reuse_port(true)?;
            socket.set_nonblocking(true)?;
            socket.bind(&address.into())?;
            socket.listen(1024)?;
            let _guard = handle.enter();
            let listener = TcpListener::from_std(socket.into())?;
            sockets.push((handle.clone(), listener));
        }
        BoundListener::new(sockets)
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub(crate) async fn bind(
    address: SocketAddr,
    protocol: prost_config::Protocol,
    workers: Option<&Workers>,
    quic_background: &TaskTracker,
) -> Result<BoundListener> {
    if protocol == prost_config::Protocol::Http3 {
        let socket = std::net::UdpSocket::bind(address)?;
        socket.set_nonblocking(true)?;
        let endpoint = quinn::Endpoint::new(
            quinn::EndpointConfig::default(),
            None,
            socket,
            Arc::new(QuicRuntime(quic_background.clone())),
        )?;
        return Ok(BoundListener {
            sockets: Vec::new(),
            masters: Masters::Quic(endpoint.clone()),
            udp: None,
            quic: Some(endpoint),
        });
    }
    if protocol == prost_config::Protocol::Udp {
        let socket = UdpSocket::bind(address).await?;
        let master = std::net::UdpSocket::from(socket.as_fd().try_clone_to_owned()?);
        let sessions = Arc::new(crate::udp::Sessions::default());
        return Ok(BoundListener {
            sockets: Vec::new(),
            masters: Masters::Udp(master, sessions.clone()),
            udp: Some((socket, sessions)),
            quic: None,
        });
    }
    match workers {
        Some(workers) => workers.bind(address),
        None => BoundListener::new(vec![(Handle::current(), TcpListener::bind(address).await?)]),
    }
}
pub(crate) fn attach(masters: &Masters, workers: Option<&Workers>) -> Result<BoundListener> {
    if let Some(workers) = workers {
        return workers.attach(masters);
    }
    let Masters::Tcp(masters) = masters else {
        return attach_udp(masters);
    };
    anyhow::ensure!(masters.len() == 1, "shared runtime listener count mismatch");
    BoundListener::new(vec![(
        Handle::current(),
        TcpListener::from_std(masters[0].try_clone()?)?,
    )])
}
fn attach_udp(masters: &Masters) -> Result<BoundListener> {
    if let Masters::Quic(endpoint) = masters {
        return Ok(BoundListener {
            sockets: Vec::new(),
            masters: Masters::Quic(endpoint.clone()),
            udp: None,
            quic: Some(endpoint.clone()),
        });
    }
    let Masters::Udp(master, sessions) = masters else {
        anyhow::bail!("expected UDP socket")
    };
    let socket = UdpSocket::from_std(master.try_clone()?)?;
    Ok(BoundListener {
        sockets: Vec::new(),
        masters: Masters::Udp(master.try_clone()?, sessions.clone()),
        udp: Some((socket, sessions.clone())),
        quic: None,
    })
}
