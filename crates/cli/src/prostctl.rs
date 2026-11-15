use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use prost_control::{
    request, CacheKeyInput, CacheQuery, Command, KeyEncoding, RateTableTarget, TableKey, TableQuery,
};
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
    Workers {
        #[command(subcommand)]
        command: Workers,
    },
    Validate {
        #[arg(long)]
        file: PathBuf,
    },
    #[command(alias = "reload")]
    Apply {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        if_generation: Option<u64>,
        /// Wait for older workers to drain after committing the configuration.
        #[arg(long)]
        wait: bool,
        #[arg(long, requires = "wait", value_parser = clap::value_parser!(u64).range(1..=300))]
        wait_timeout: Option<u64>,
    },
    Upstream {
        #[command(subcommand)]
        command: Upstream,
    },
    Config {
        #[command(subcommand)]
        command: Config,
    },
    Tls {
        #[command(subcommand)]
        command: Tls,
    },
    Artifacts {
        #[command(subcommand)]
        command: Artifacts,
    },
    AccessLog {
        #[command(subcommand)]
        command: AccessLog,
    },
    RateTable {
        #[command(subcommand)]
        command: RateTable,
    },
    Cache {
        #[command(subcommand)]
        command: Cache,
    },
}
#[derive(Subcommand)]
enum Tls {
    /// Replace frontend TLS configuration without draining existing connections.
    Reload {
        listener: String,
        /// TOML containing certificate/private_key and optional SNI/policy fields.
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        if_generation: Option<u64>,
    },
}
#[derive(Subcommand)]
enum Workers {
    /// Wait for workers older than a committed generation without changing config.
    Wait {
        #[arg(long)]
        generation: u64,
        #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=300))]
        timeout: u64,
    },
}
#[derive(Subcommand)]
enum Cache {
    Status {
        upstream: Option<String>,
    },
    Entries {
        upstream: String,
        #[arg(long)]
        key: Option<String>,
        #[arg(long,default_value_t=100,value_parser=clap::value_parser!(u16).range(1..=1024))]
        limit: u16,
    },
    Key {
        #[arg(long)]
        uri: String,
        #[arg(long)]
        host: Option<String>,
        #[arg(long)]
        accept_encoding: Vec<String>,
    },
    #[command(group(clap::ArgGroup::new("cache_scope").args(["key","all"]).required(true)))]
    Purge {
        upstream: String,
        #[arg(long, conflicts_with = "all")]
        key: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        if_generation: Option<u64>,
    },
}
#[derive(clap::Args)]
struct RateTarget {
    listener: String,
    #[arg(long)]
    route: Option<usize>,
    #[arg(long)]
    plugin: usize,
}
impl From<RateTarget> for RateTableTarget {
    fn from(value: RateTarget) -> Self {
        Self {
            listener: value.listener,
            route: value.route,
            plugin: value.plugin,
        }
    }
}
#[derive(clap::Args)]
struct RateKey {
    #[arg(long)]
    key: Option<String>,
    #[arg(long, requires = "key")]
    key_hex: bool,
}
impl RateKey {
    fn into_key(self) -> Option<TableKey> {
        self.key.map(|value| TableKey {
            value,
            encoding: if self.key_hex {
                KeyEncoding::Hex
            } else {
                KeyEncoding::Text
            },
        })
    }
}
#[derive(Subcommand)]
enum RateTable {
    List {
        #[arg(long)]
        listener: Option<String>,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=1024))]
        limit: u16,
    },
    Show {
        #[command(flatten)]
        target: RateTarget,
        #[command(flatten)]
        key: RateKey,
        #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u16).range(1..=1024))]
        limit: u16,
    },
    #[command(group(clap::ArgGroup::new("clear_scope").args(["key", "all"]).required(true)))]
    Clear {
        #[command(flatten)]
        target: RateTarget,
        #[command(flatten)]
        key: RateKey,
        #[arg(long, conflicts_with = "key")]
        all: bool,
        #[arg(long)]
        if_generation: Option<u64>,
    },
}
#[derive(Subcommand)]
enum AccessLog {
    Flush {
        #[arg(long)]
        if_generation: Option<u64>,
    },
    Reopen {
        #[arg(long)]
        if_generation: Option<u64>,
    },
}
#[derive(Subcommand)]
enum Artifacts {
    /// Preview collection by default; --delete unlinks eligible managed files.
    Gc {
        #[arg(long)]
        delete: bool,
        #[arg(long, default_value_t = 3600)]
        min_age_seconds: u64,
        #[arg(long, default_value_t = 100_000)]
        max_scan_entries: usize,
        #[arg(long, default_value_t = 1024)]
        max_delete_files: u64,
        #[arg(long)]
        if_generation: Option<u64>,
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
    let wait = match &args.command {
        Ctl::Apply {
            wait: true,
            wait_timeout,
            ..
        } => Some(wait_timeout.unwrap_or(60) * 1000),
        _ => None,
    };
    let command = match args.command {
        Ctl::Status => Command::Status,
        Ctl::Workers {
            command:
                Workers::Wait {
                    generation,
                    timeout,
                },
        } => Command::WaitWorkers {
            generation,
            timeout_ms: timeout * 1000,
        },
        Ctl::Validate { file } => Command::Validate {
            config: std::fs::read_to_string(file)?,
        },
        Ctl::Apply {
            file,
            if_generation,
            ..
        } => Command::Apply {
            config: std::fs::read_to_string(file)?,
            expected_generation: generation(&args.socket, if_generation).await?,
        },
        Ctl::Config { .. } => Command::Export,
        Ctl::Tls {
            command:
                Tls::Reload {
                    listener,
                    file,
                    if_generation,
                },
        } => Command::ReloadTls {
            listener,
            tls: Box::new(prost_config::ServerTls::parse(&std::fs::read_to_string(
                file,
            )?)?),
            expected_generation: generation(&args.socket, if_generation).await?,
        },
        Ctl::Cache { command } => match command {
            Cache::Status { upstream } => Command::CacheStatus { upstream },
            Cache::Entries {
                upstream,
                key,
                limit,
            } => Command::CacheEntries {
                upstream,
                query: CacheQuery {
                    key,
                    limit: limit as usize,
                },
            },
            Cache::Key {
                uri,
                host,
                accept_encoding,
            } => Command::CacheKey {
                input: CacheKeyInput {
                    uri,
                    host,
                    accept_encoding,
                },
            },
            Cache::Purge {
                upstream,
                key,
                all,
                if_generation,
            } => {
                anyhow::ensure!(key.is_some() != all, "select exactly one of --key or --all");
                Command::CachePurge {
                    upstream,
                    key,
                    all,
                    expected_generation: generation(&args.socket, if_generation).await?,
                }
            }
        },
        Ctl::RateTable { command } => match command {
            RateTable::List { listener, limit } => Command::RateTableList {
                listener,
                limit: limit as usize,
            },
            RateTable::Show { target, key, limit } => Command::RateTableShow {
                target: target.into(),
                query: TableQuery {
                    key: key.into_key(),
                    limit: limit as usize,
                },
            },
            RateTable::Clear {
                target,
                key,
                all,
                if_generation,
            } => {
                let key = key.into_key();
                anyhow::ensure!(key.is_some() != all, "select exactly one of --key or --all");
                Command::RateTableClear {
                    target: target.into(),
                    key,
                    all,
                    expected_generation: generation(&args.socket, if_generation).await?,
                }
            }
        },
        Ctl::AccessLog { command } => match command {
            AccessLog::Flush { if_generation } => Command::FlushAccessLog {
                expected_generation: generation(&args.socket, if_generation).await?,
            },
            AccessLog::Reopen { if_generation } => Command::ReopenAccessLog {
                expected_generation: generation(&args.socket, if_generation).await?,
            },
        },
        Ctl::Artifacts {
            command:
                Artifacts::Gc {
                    delete,
                    min_age_seconds,
                    max_scan_entries,
                    max_delete_files,
                    if_generation,
                },
        } => Command::CollectArtifacts {
            dry_run: !delete,
            min_age_seconds,
            max_entries: max_scan_entries,
            max_delete_files,
            expected_generation: generation(&args.socket, if_generation).await?,
        },
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
    let mut response = request(&args.socket, command).await?;
    if let Some(timeout_ms) = wait {
        let committed = response.generation;
        match request(
            &args.socket,
            Command::WaitWorkers {
                generation: committed,
                timeout_ms,
            },
        )
        .await
        {
            Ok(drained) => {
                if let Some(result) = response.result.as_mut() {
                    result["worker_drain"] = drained.result.context("missing drain result")?;
                }
            }
            Err(error) => {
                println!("{}", serde_json::to_string_pretty(&response)?);
                return Err(error.context(format!("configuration generation {committed} committed; worker wait failed; inspect status before retrying")));
            }
        }
    }
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
        if let Some(result) = &response.result {
            anyhow::ensure!(
                result.get("errors").and_then(|v| v.as_u64()).unwrap_or(0) == 0
                    && result
                        .get("durable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true),
                "operation incomplete or not durable; inspect result"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;
    #[test]
    fn rolling_reload_wait_cli_validates_timeout_and_alias() -> Result<()> {
        Args::try_parse_from(["prostctl", "workers", "wait", "--generation", "12"])?;
        assert!(Args::try_parse_from([
            "prostctl",
            "workers",
            "wait",
            "--generation",
            "12",
            "--timeout",
            "0"
        ])
        .is_err());
        let args = Args::try_parse_from([
            "prostctl",
            "reload",
            "--file",
            "candidate.toml",
            "--wait",
            "--wait-timeout",
            "90",
        ])?;
        assert!(matches!(
            args.command,
            Ctl::Apply {
                wait: true,
                wait_timeout: Some(90),
                ..
            }
        ));
        for extra in [
            vec!["--wait-timeout", "10"],
            vec!["--wait", "--wait-timeout", "0"],
            vec!["--wait", "--wait-timeout", "301"],
        ] {
            let mut command = vec!["prostctl", "apply", "--file", "candidate.toml"];
            command.extend(extra);
            assert!(Args::try_parse_from(command).is_err());
        }
        Ok(())
    }
    #[test]
    fn cache_cli_requires_scope_bounds_and_preserves_header_occurrences() -> Result<()> {
        Args::command().debug_assert();
        for extra in [vec![], vec!["--key", "abc", "--all"]] {
            let mut command = vec!["prostctl", "cache", "purge", "app"];
            command.extend(extra);
            assert!(Args::try_parse_from(command).is_err());
        }
        for limit in ["0", "1025"] {
            assert!(Args::try_parse_from([
                "prostctl", "cache", "entries", "app", "--limit", limit
            ])
            .is_err());
        }
        let args = Args::try_parse_from([
            "prostctl",
            "cache",
            "key",
            "--uri",
            "/a",
            "--host",
            "Cache.Test",
            "--accept-encoding",
            "gzip",
            "--accept-encoding",
            "br",
        ])?;
        let Ctl::Cache {
            command:
                Cache::Key {
                    uri,
                    host,
                    accept_encoding,
                },
        } = args.command
        else {
            panic!("wrong command");
        };
        assert_eq!(uri, "/a");
        assert_eq!(host.as_deref(), Some("Cache.Test"));
        assert_eq!(accept_encoding, vec!["gzip", "br"]);
        Args::try_parse_from(["prostctl", "cache", "status"])?;
        Args::try_parse_from([
            "prostctl",
            "cache",
            "purge",
            "app",
            "--all",
            "--if-generation",
            "12",
        ])?;
        Ok(())
    }
    #[test]
    fn rate_table_cli_requires_explicit_clear_scope_and_valid_bounds() {
        Args::command().debug_assert();
        for extra in [vec![], vec!["--key", "alice", "--all"], vec!["--key-hex"]] {
            let mut command = vec!["prostctl", "rate-table", "clear", "tcp", "--plugin", "0"];
            command.extend(extra);
            assert!(Args::try_parse_from(command).is_err());
        }
        for limit in ["0", "1025"] {
            assert!(Args::try_parse_from([
                "prostctl",
                "rate-table",
                "show",
                "tcp",
                "--plugin",
                "0",
                "--limit",
                limit
            ])
            .is_err());
        }
    }
    #[test]
    fn rate_table_cli_supports_discovery_route_and_binary_keys() -> Result<()> {
        Args::try_parse_from(["prostctl", "rate-table", "list", "--listener", "edge"])?;
        let parsed = Args::try_parse_from([
            "prostctl",
            "rate-table",
            "clear",
            "edge",
            "--route",
            "2",
            "--plugin",
            "1",
            "--key",
            "80ff",
            "--key-hex",
            "--if-generation",
            "12",
        ])?;
        let Ctl::RateTable {
            command:
                RateTable::Clear {
                    target,
                    key,
                    if_generation,
                    ..
                },
        } = parsed.command
        else {
            panic!("wrong command");
        };
        assert_eq!(target.route, Some(2));
        assert_eq!(target.plugin, 1);
        assert_eq!(if_generation, Some(12));
        let key = key.into_key().unwrap();
        assert!(matches!(key.encoding, KeyEncoding::Hex));
        assert_eq!(key.value, "80ff");
        Ok(())
    }
}
