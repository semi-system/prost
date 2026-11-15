mod agent;
mod config;
mod delivery;
mod dns01;
mod http;
mod status;
mod store;
mod webhook;

use anyhow::{ensure, Result};
use clap::{Parser, Subcommand};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about = "Independent HTTP-01 certificate agent for Prost")]
struct Args {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Run {
        #[arg(long)]
        config: PathBuf,
    },
    Bootstrap {
        #[arg(long)]
        config: PathBuf,
    },
    Check {
        #[arg(long, default_value = "127.0.0.1:8088")]
        address: std::net::SocketAddr,
        #[arg(long)]
        certificates: bool,
    },
}
#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    if let Command::Check {
        address,
        certificates,
    } = args.command
    {
        let io = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::TcpStream::connect(address),
        )
        .await??;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let response = tokio::time::timeout(
            Duration::from_secs(3),
            sender.send_request(
                ::http::Request::builder()
                    .uri(if certificates { "/readyz" } else { "/healthz" })
                    .header("host", "localhost")
                    .body(http_body_util::Empty::<hyper::body::Bytes>::new())?,
            ),
        )
        .await??;
        ensure!(response.status() == 200, "certificate agent is not ready");
        driver.abort();
        return Ok(());
    }
    let (path, run) = match args.command {
        Command::Run { config } => (config, true),
        Command::Bootstrap { config } => (config, false),
        Command::Check { .. } => unreachable!(),
    };
    let config = config::Config::parse(&std::fs::read_to_string(path)?)?;
    let store = Arc::new(store::Store::open(&config)?);
    let bundles = config
        .certificates
        .iter()
        .map(|spec| store.bootstrap(spec))
        .collect::<Result<Vec<_>>>()?;
    if !run {
        return Ok(());
    }
    let listener = tokio::net::TcpListener::bind(config.challenge_address).await?;
    let state = Arc::new(http::State::default());
    *state.certificates.write().unwrap() = status::initial(&config, &bundles);
    state.readiness_margin.store(
        config.readiness_min_validity_seconds,
        std::sync::atomic::Ordering::Relaxed,
    );
    let stop = CancellationToken::new();
    let server = tokio::spawn(http::serve(listener, state.clone(), stop.clone()));
    let worker = tokio::spawn(agent::maintain(config, store, state, bundles, stop.clone()));
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
    stop.cancel();
    worker.abort();
    let _ = worker.await;
    let _ = server.await;
    Ok(())
}
