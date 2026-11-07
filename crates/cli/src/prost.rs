use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
#[derive(Parser)]
#[command(version, about = "Prost L4/L7 reverse proxy")]
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
    Check {
        #[arg(long)]
        config: PathBuf,
    },
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Args::parse().command {
        Command::Check { config } => {
            prost_config::Config::parse(&std::fs::read_to_string(config)?)?;
            println!(
                "configuration valid (schema only; use prostctl validate for TLS/WASM checks)"
            );
        }
        Command::Run { config } => {
            let proxy = prost_core::ProxyBuilder::new(config)?.start().await?;
            let control = match prost_control::ControlServer::bind(proxy.clone()).await {
                Ok(v) => v,
                Err(e) => {
                    proxy.shutdown().await;
                    return Err(e);
                }
            };
            tracing::info!(status=%proxy.status(),"proxy ready");
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
            control.shutdown().await;
            proxy.shutdown().await;
        }
    }
    Ok(())
}
