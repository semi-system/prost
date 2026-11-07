"""Build release binaries in Docker/WSL, then layer them onto the MVP image."""
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent


def run(*args):
    subprocess.run(args, check=True)


# Use a disposable export container; Cargo caches live in Linux named volumes.
name = "prost-release-export"
run("docker", "run", "--name", name,
    "-v", f"{ROOT}:/work", "-v", "prost-target:/work/target",
    "-v", "prost-cargo:/usr/local/cargo/registry", "-w", "/work",
    "-e", "CARGO_BUILD_JOBS=2", "rust:1.93-bookworm",
    "cargo", "build", "--release", "--locked", "--workspace")
try:
    with tempfile.TemporaryDirectory(prefix="prost-release-image-") as directory:
        context = Path(directory)
        for binary in ("prost", "prostctl"):
            run("docker", "cp", f"{name}:/work/target/release/{binary}", str(context / binary))
        (context / "Dockerfile").write_text(
            "FROM prost:mvp\nCOPY prost prostctl /usr/local/bin/\n")
        run("docker", "build", "-t", "prost:bench-release", str(context))
finally:
    run("docker", "rm", name)
