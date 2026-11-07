use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use prost_control::{request, Command};
use std::path::PathBuf;
#[derive(Parser)]
#[command(version, about = "Prost Unix socket administration")]
struct Args {
    #[arg(long, global = true, default_value = "/run/prost/admin.sock")]
    socket: PathBuf,
    #[command(subcommand)]
    command: Ctl,
}
#[derive(Subcommand)]
enum Ctl {
    Status,
    Validate {
        #[arg(long)]
        file: PathBuf,
    },
    Apply {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        if_generation: Option<u64>,
    },
    Upstream {
        #[command(subcommand)]
        command: Upstream,
    },
    Config {
        #[command(subcommand)]
        command: Config,
    },
}
#[derive(Subcommand)]
enum Upstream {
    Enable {
        endpoint: String,
        #[arg(long)]
        if_generation: Option<u64>,
    },
    Disable {
        endpoint: String,
        #[arg(long)]
        if_generation: Option<u64>,
    },
}
#[derive(Subcommand)]
enum Config {
    Export,
}
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    async fn generation(socket: &std::path::Path, n: Option<u64>) -> Result<u64> {
        match n {
            Some(n) => Ok(n),
            None => Ok(request(socket, Command::Status).await?.generation),
        }
    }
    let export = matches!(args.command, Ctl::Config { .. });
    let command = match args.command {
        Ctl::Status => Command::Status,
        Ctl::Validate { file } => Command::Validate {
            config: std::fs::read_to_string(file)?,
        },
        Ctl::Apply {
            file,
            if_generation,
        } => Command::Apply {
            config: std::fs::read_to_string(file)?,
            expected_generation: generation(&args.socket, if_generation).await?,
        },
        Ctl::Config { .. } => Command::Export,
        Ctl::Upstream { command } => {
            let (endpoint, enabled, n) = match command {
                Upstream::Enable {
                    endpoint,
                    if_generation,
                } => (endpoint, true, if_generation),
                Upstream::Disable {
                    endpoint,
                    if_generation,
                } => (endpoint, false, if_generation),
            };
            Command::SetEnabled {
                endpoint,
                enabled,
                expected_generation: generation(&args.socket, n).await?,
            }
        }
    };
    let response = request(&args.socket, command).await?;
    if export {
        print!(
            "{}",
            response
                .result
                .as_ref()
                .and_then(|v| v.get("config"))
                .and_then(|v| v.as_str())
                .context("missing config")?
        );
    } else {
        println!("{}", serde_json::to_string_pretty(&response)?);
    }
    Ok(())
}
