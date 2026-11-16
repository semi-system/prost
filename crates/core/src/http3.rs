//! QUIC listeners reuse the HTTP pipeline and keep an endpoint across reloads.
use super::*;
use bytes::{Buf, Bytes};
use http_body_util::BodyExt;
use hyper::body::Frame;
use prost_transport::quic as transport;
use tokio::{sync::mpsc, task::JoinSet};

pub(crate) struct Prepared {
    server: Arc<quinn::ServerConfig>,
    capacity: Arc<Semaphore>,
    settings: prost_config::Http3Settings,
}
struct CloseOnDrop(quinn::Connection);
impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0
            .close(quinn::VarInt::from_u32(0x100), b"HTTP/3 closed");
    }
}
pub(crate) fn prepare(
    listener: &Listener,
    tls: &Arc<rustls::ServerConfig>,
    previous: Option<&PreparedListener>,
) -> Result<Prepared> {
    let settings = listener.http3.clone().unwrap_or_default();
    let mut tls = (**tls).clone();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    tls.max_early_data_size = 0;
    let crypto = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .context("HTTP/3 TLS policy requires TLS1.3 AES128-GCM")?;
    let mut transport = quinn::TransportConfig::default();
    transport
        .max_concurrent_bidi_streams(settings.max_streams.into())
        .max_concurrent_uni_streams(8u32.into())
        .stream_receive_window(settings.stream_receive_window.into())
        .receive_window(settings.connection_receive_window.into())
        .send_window(settings.send_window as u64)
        .max_idle_timeout(Some(
            Duration::from_millis(settings.idle_timeout_ms).try_into()?,
        ))
        .datagram_receive_buffer_size(None);
    let mut server = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    server
        .transport_config(Arc::new(transport))
        .migration(false)
        .max_incoming(settings.max_connections.saturating_mul(2))
        .incoming_buffer_size(65536)
        .incoming_buffer_size_total(4 * 1024 * 1024);
    let capacity = previous
        .and_then(|p| p.http3.as_ref())
        .filter(|old| old.settings.max_connections == settings.max_connections)
        .map(|old| old.capacity.clone())
        .unwrap_or_else(|| Arc::new(Semaphore::new(settings.max_connections)));
    Ok(Prepared {
        server: Arc::new(server),
        capacity,
        settings,
    })
}

impl RunningProxy {
    pub(crate) fn refresh_quic_tls(&self, snapshot: &Snapshot) {
        for (id, binding) in self.bindings.lock().unwrap().iter() {
            if let workers::Masters::Quic(endpoint) = &binding.masters {
                endpoint.set_server_config(
                    snapshot
                        .listeners
                        .get(id)
                        .and_then(|l| l.http3.as_ref())
                        .map(|p| (*p.server).clone()),
                );
            }
        }
    }
    pub(crate) fn start_http3(
        self: &Arc<Self>,
        id: String,
        endpoint: quinn::Endpoint,
        generation: Arc<rolling::Generation>,
        cancel: CancellationToken,
        snapshot: &Snapshot,
    ) {
        let prepared = snapshot.listeners[&id]
            .http3
            .as_ref()
            .expect("prepared QUIC listener");
        endpoint.set_server_config(Some((*prepared.server).clone()));
        let weak = Arc::downgrade(self);
        let accepts = generation.accepts.clone();
        self.tasks.spawn(accepts.track_future(async move {
            loop {
                let incoming=tokio::select!{biased;_=cancel.cancelled()=>break,_=generation.accepting.cancelled()=>break,r=endpoint.accept()=>r};
                let Some(incoming)=incoming else{break};let Some(p)=weak.upgrade() else{break};
                let snapshot=if generation.rolling {generation.snapshot.load_full().expect("QUIC accept snapshot retained")}else{p.snapshot()};
                let Some(listener)=snapshot.listeners.get(&id).cloned() else{incoming.refuse();continue};
                let prepared=listener.http3.as_ref().expect("QUIC protocol stable");
                let peer=incoming.remote_address();
                let admitted=(||->Result<_> {
                    let global=p.connections.clone().try_acquire_owned()?;
                    let local=listener.capacity.clone().try_acquire_owned()?;
                    let ip=listener.ip_capacity.acquire(peer.ip(),listener.config.max_connections_per_ip)?;
                    let quic=prepared.capacity.clone().try_acquire_owned()?;
                    let memory=p.quic_receive.clone().try_acquire_many_owned(prepared.settings.connection_receive_window)?;
                    let handshake=p.handshakes.clone().try_acquire_owned()?;
                    let plugins=connection_permits(&listener.plugins)?;
                    Ok((global,local,ip,quic,memory,handshake,plugins))
                })();
                let (global,local,ip,quic,memory,handshake,plugins)=match admitted {
                    Ok(permits)=>permits,Err(_)=>{p.observed.tls[3].fetch_add(1,Ordering::Relaxed);incoming.refuse();continue;}
                };
                p.metrics.connections.inc();generation.connections.fetch_add(1,Ordering::Relaxed);
                let lease=ConnectionLease {generation:generation.clone(),_ip:Some(ip),audit:None,_global:global,_local:local,_plugins:plugins,active:p.metrics.connections.clone()};
                let proxy=p.clone();let id=id.clone();let local_address=endpoint.local_addr().expect("bound QUIC address");
                let force=generation.force.clone();
                p.tasks.spawn(generation.tasks.track_future(async move {
                    let _quic=quic;let _memory=memory;
                    let result=tokio::select! {biased;_=proxy.force.cancelled()=>Ok(()),_=force.cancelled()=>Ok(()),r=proxy.quic_connection(id,incoming,peer,local_address,snapshot,listener,lease,handshake)=>r};
                    if let Err(error)=result {proxy.metrics.errors.inc();tracing::debug!(%error,%peer,"HTTP/3 connection ended");}
                }));
            }
            if let Some(p)=weak.upgrade() {
                if p.shutdown.is_cancelled() || !p.snapshot().listeners.contains_key(&id) {endpoint.set_server_config(None);}
            }
        }));
    }
    #[allow(clippy::too_many_arguments)]
    async fn quic_connection(
        self: &Arc<Self>,
        id: String,
        incoming: quinn::Incoming,
        peer: SocketAddr,
        local: SocketAddr,
        snapshot: Arc<Snapshot>,
        listener: Arc<PreparedListener>,
        lease: ConnectionLease,
        handshake: OwnedSemaphorePermit,
    ) -> Result<()> {
        for index in &listener.connection_admission {
            invoke_http_plugins(
                std::slice::from_ref(&listener.plugins[*index]),
                Phase::new("connection", peer.ip()),
                None,
                None,
            )
            .await?;
        }
        let mut timer = observability::Handshake::new(&self.observed, &self.metrics.tls_latency);
        let prepared = listener.http3.as_ref().context("HTTP/3 not prepared")?;
        let connection = match tokio::time::timeout(
            Duration::from_millis(listener.config.tls_handshake_timeout_ms),
            incoming.accept_with(prepared.server.clone())?,
        )
        .await
        {
            Ok(Ok(connection)) => {
                timer.finish(0);
                connection
            }
            Ok(Err(error)) => {
                timer.finish(1);
                return Err(error.into());
            }
            Err(error) => {
                timer.finish(2);
                return Err(error.into());
            }
        };
        drop(handshake);
        let _close = CloseOnDrop(connection.clone());
        if listener.has_hooks {
            let mut event = Event {
                abi_version: 1,
                hook: "connection".into(),
                peer: peer.to_string(),
                upstream: listener.config.upstream.clone(),
                headers: BTreeMap::new(),
                config: serde_json::Value::Null,
            };
            invoke(&listener.plugins, &mut event, None).await?;
        }
        let info = Arc::new(ConnectionInfo {
            snapshot: lease.generation.rolling.then(|| snapshot.clone()),
            addresses: prost_transport::ProxyAddresses {
                source: peer,
                destination: local,
            },
            proxy_clients: proxy_clients::Clients::new(self.proxy_connections.clone()),
            transport_peer: peer,
            headers_seen: std::sync::atomic::AtomicBool::new(false),
            first_headers: tokio::sync::Notify::new(),
            peer,
            peer_text: peer.to_string(),
            forwarded_for: HeaderValue::from_str(&peer.ip().to_string())?,
            forwarded_proto: HeaderValue::from_static("https"),
            _lease: lease,
        });
        let mut builder = h3::server::builder();
        builder.max_field_section_size(32768);
        let signals = Arc::new(transport::CancellationSet::new(
            prepared.settings.max_streams as usize,
        ));
        let mut http = builder
            .build::<_, Bytes>(transport::Connection::new(
                connection.clone(),
                signals.clone(),
            ))
            .await?;
        let mut jobs = JoinSet::new();
        let mut draining = false;
        let id: Arc<str> = id.into();
        loop {
            if draining && jobs.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _=self.shutdown.cancelled(),if !draining=>{http.shutdown(0).await?;draining=true;},
                _=info._lease.generation.drain.cancelled(),if !draining=>{http.shutdown(0).await?;draining=true;},
                Some(result)=jobs.join_next(),if !jobs.is_empty()=>{if let Err(error)=result {tracing::debug!(%error,"HTTP/3 stream task failed");}},
                result=http.accept()=>{
                    match result {
                        Ok(Some(resolver))=>{
                            let proxy=self.clone();let id=id.clone();let info=info.clone();let header_ms=listener.config.header_read_timeout_ms;
                            let write_ms=listener.config.client_write_timeout_ms.unwrap_or(prepared.settings.stream_write_timeout_ms);
                            let signals=signals.clone();let closed=connection.clone();let errors=self.metrics.errors.clone();
                            jobs.spawn(async move {
                                let result=tokio::select!{biased;_=closed.closed()=>Ok(()),r=proxy.quic_request(id,info,resolver,signals,header_ms,write_ms)=>r};
                                if let Err(error)=result {errors.inc();tracing::debug!(%error,"HTTP/3 stream ended");}
                            });
                        },
                        Ok(None)=>break,
                        Err(error)=>{tracing::debug!(%error,"HTTP/3 accept ended");connection.close(quinn::VarInt::from_u32(0x102),b"HTTP/3 accept ended");break},
                    }
                }
            }
        }
        while jobs.join_next().await.is_some() {}
        signals.wait_delivered().await;
        if draining {
            // Give the queued GOAWAY control frame an opportunity to leave the
            // endpoint, including an idle connection with no request FINs.
            tokio::time::sleep(
                connection
                    .rtt()
                    .saturating_mul(3)
                    .clamp(Duration::from_millis(10), Duration::from_secs(1)),
            )
            .await;
        }
        connection.close(quinn::VarInt::from_u32(0x100), b"HTTP/3 drained");
        Ok(())
    }

    async fn quic_request(
        self: Arc<Self>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        resolver: h3::server::RequestResolver<transport::Connection, Bytes>,
        signals: transport::Cancellations,
        header_ms: u64,
        write_ms: u64,
    ) -> Result<()> {
        let resolved = resolver.resolve_request();
        let (request, stream) = if header_ms == 0 {
            resolved.await?
        } else {
            tokio::time::timeout(Duration::from_millis(header_ms), resolved)
                .await
                .context("HTTP/3 request header timeout")??
        };
        let Some(cancel) = signals.get(stream.send_id().into_inner()) else {
            return Ok(());
        };
        tokio::select! {biased;_=cancel.cancelled()=>Ok(()),result=self.quic_stream(id,info,request,stream,write_ms)=>result}
    }
    async fn quic_stream(
        self: Arc<Self>,
        id: Arc<str>,
        info: Arc<ConnectionInfo>,
        mut request: Request<()>,
        stream: h3::server::RequestStream<transport::Bidi, Bytes>,
        write_ms: u64,
    ) -> Result<()> {
        *request.version_mut() = http::Version::HTTP_3;
        if !request.headers().contains_key(header::HOST) {
            if let Some(authority) = request.uri().authority() {
                let value = HeaderValue::from_str(authority.as_str())?;
                request.headers_mut().insert(header::HOST, value);
            }
        }
        let (mut send, mut recv) = stream.split();
        let length = match content_length(request.headers()) {
            Ok(length) => length,
            Err(_) => {
                recv.stop_sending(h3::error::Code::H3_MESSAGE_ERROR);
                send.send_response(Response::builder().status(400).body(())?)
                    .await?;
                send.finish().await?;
                return Ok(());
            }
        };
        let (tx, rx) = mpsc::channel(1);
        let force = self.force.clone();
        let worker_force = info._lease.generation.force.clone();
        let producer=self.tasks.spawn(info._lease.generation.tasks.track_future(async move {
            let cancelled=tokio::select!{biased;_=force.cancelled()=>true,_=worker_force.cancelled()=>true,_=async {
                if receive(&mut recv,&tx,length).await.is_err() {recv.stop_sending(h3::error::Code::H3_MESSAGE_ERROR);let _=tx.send(Err(Box::new(prost_http::HttpFailure::InvalidBody) as prost_http::Error)).await;}
            }=>false};
            if cancelled {recv.stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);}
        }));
        let request = request.map(|_| IncomingBody::frames(rx, length, producer.abort_handle()));
        let head = request.method() == http::Method::HEAD;
        let response = if self.access_log.is_some() {
            self.handle_http_logged(id, info, request, false).await
        } else {
            self.handle_http(id, info, request, None).await
        };
        let suppress = head
            || response.status().is_informational()
            || matches!(response.status().as_u16(), 204 | 304);
        let (mut parts, body) = response.into_parts();
        parts.version = http::Version::HTTP_3;
        let mut body = Box::pin(body);
        let writing = async {
            write(
                write_ms,
                send.send_response(Response::from_parts(parts, ())),
            )
            .await?;
            if !suppress {
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(anyhow::Error::from_boxed)?;
                    match frame.into_data() {
                        Ok(mut data) => {
                            while data.has_remaining() {
                                let size = data.remaining().min(16384);
                                write(write_ms, send.send_data(data.split_to(size))).await?;
                            }
                        }
                        Err(frame) => {
                            if let Ok(trailers) = frame.into_trailers() {
                                write(write_ms, send.send_trailers(trailers)).await?;
                                write(write_ms, send.finish()).await?;
                                return Ok::<_, anyhow::Error>(());
                            }
                        }
                    }
                }
            }
            write(write_ms, send.finish()).await?;
            Ok(())
        };
        let result = writing.await;
        if result.is_err() {
            send.stop_stream(h3::error::Code::H3_INTERNAL_ERROR);
        }
        result
    }
}

async fn write<T, E: Into<anyhow::Error>>(
    ms: u64,
    future: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    if ms == 0 {
        future.await.map_err(Into::into)
    } else {
        tokio::time::timeout(Duration::from_millis(ms), future)
            .await
            .context("HTTP/3 response write timeout")?
            .map_err(Into::into)
    }
}

fn content_length(headers: &http::HeaderMap) -> Result<Option<u64>> {
    let mut length = None;
    for value in headers.get_all(header::CONTENT_LENGTH) {
        for item in value.to_str()?.split(',') {
            let item = item.trim();
            ensure!(
                !item.is_empty() && item.bytes().all(|b| b.is_ascii_digit()),
                "invalid HTTP/3 content-length"
            );
            let next = item.parse::<u64>()?;
            ensure!(
                length.is_none_or(|n| n == next),
                "conflicting HTTP/3 content-length"
            );
            length = Some(next);
        }
    }
    Ok(length)
}
async fn receive(
    recv: &mut h3::server::RequestStream<transport::Recv, Bytes>,
    tx: &mpsc::Sender<Result<Frame<Bytes>, prost_http::Error>>,
    length: Option<u64>,
) -> Result<()> {
    let mut received = 0u64;
    while let Some(mut data) = recv.recv_data().await? {
        received = received
            .checked_add(data.remaining() as u64)
            .context("HTTP/3 body size overflow")?;
        ensure!(
            length.is_none_or(|expected| received <= expected),
            "HTTP/3 body exceeds content-length"
        );
        while data.has_remaining() {
            let size = data.remaining().min(16384);
            if tx
                .send(Ok(Frame::data(data.copy_to_bytes(size))))
                .await
                .is_err()
            {
                return Ok(());
            }
        }
    }
    ensure!(
        length.is_none_or(|expected| received == expected),
        "HTTP/3 body shorter than content-length"
    );
    if let Some(trailers) = recv.recv_trailers().await? {
        let _ = tx.send(Ok(Frame::trailers(trailers))).await;
    }
    Ok(())
}
