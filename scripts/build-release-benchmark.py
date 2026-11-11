"""Build release binaries in Docker/WSL, then layer them onto the MVP image."""
from pathlib import Path
import argparse
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--profile", choices=["dev", "release"], default="release")
parser.add_argument("--tag")
parser.add_argument("--features", default="", help="Optional Cargo features, e.g. prost-cli/wasm")
args = parser.parse_args()
tag = args.tag or ("prost:bench-debug" if args.profile == "dev" else "prost:bench-release")


def run(*args):
    subprocess.run(args, check=True)


# Use a disposable export container; Cargo caches live in Linux named volumes.
name = "prost-release-export"
run("docker", "run", "--name", name,
    "-v", f"{ROOT}:/work", "-v", "prost-target:/work/target",
    "-v", "prost-cargo:/usr/local/cargo/registry", "-w", "/work",
    "-e", "CARGO_BUILD_JOBS=2", "rust:1.93-bookworm",
    "cargo", "build", "--profile", args.profile, "--locked", "--workspace", "--features", args.features)
try:
    with tempfile.TemporaryDirectory(prefix="prost-release-image-") as directory:
        context = Path(directory)
        for binary in ("prost", "prostctl"):
            profile_dir = "debug" if args.profile == "dev" else "release"
            run("docker", "cp", f"{name}:/work/target/{profile_dir}/{binary}", str(context / binary))
        (context / "Dockerfile").write_text(
            "FROM prost:mvp\nCOPY prost prostctl /usr/local/bin/\n")
        run("docker", "build", "-t", tag, str(context))
finally:
    run("docker", "rm", name)
