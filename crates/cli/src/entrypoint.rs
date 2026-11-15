//! Shell-free container launcher. exec preserves PID 1, signals and exit codes.
use std::{
    os::unix::process::CommandExt,
    process::{Command, ExitCode},
};

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let mut command = match args.next() {
        Some(program) => {
            let mut command = Command::new(program);
            command.args(args);
            command
        }
        None => {
            let mut command = Command::new("/usr/local/bin/prost");
            command.args([
                "run",
                "--config",
                "/var/lib/prost/prost.toml",
                "--seed",
                "/etc/prost/seed.toml",
            ]);
            command
        }
    };
    eprintln!("prost-entrypoint: {}", command.exec());
    ExitCode::FAILURE
}
