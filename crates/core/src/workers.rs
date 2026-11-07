use anyhow::{Context, Result};
use std::{net::SocketAddr, sync::Mutex, thread::JoinHandle, time::Duration};
use tokio::{net::TcpListener, runtime::Handle};
use tokio_util::sync::CancellationToken;

pub(crate) struct Workers {
    handles: Vec<Handle>,
    stop: CancellationToken,
    threads: Mutex<Vec<JoinHandle<()>>>,
}
pub(crate) struct BoundListener(pub Vec<(Handle, TcpListener)>);

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
        Ok(BoundListener(sockets))
    }
}
impl Drop for Workers {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

pub(crate) async fn bind(address: SocketAddr, workers: Option<&Workers>) -> Result<BoundListener> {
    match workers {
        Some(workers) => workers.bind(address),
        None => Ok(BoundListener(vec![(
            Handle::current(),
            TcpListener::bind(address).await?,
        )])),
    }
}
