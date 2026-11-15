"""Build release binaries in Docker/WSL, then layer them onto the MVP image."""
from pathlib import Path
import argparse
import subprocess
import tempfile
import json
import uuid

ROOT = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--profile", choices=["dev", "release"], default="release")
parser.add_argument("--tag")
parser.add_argument("--features", default="", help="Optional Cargo features, e.g. prost-cli/wasm")
parser.add_argument("--base-image", default="prost:mvp", help="Existing compatible runtime image to receive the built binaries")
parser.add_argument("--target", help="Override compilation target, e.g. x86_64-unknown-linux-musl")
parser.add_argument("--builder-image", help="Override the Rust builder/toolchain image")
args = parser.parse_args()
tag = args.tag or ("prost:bench-debug" if args.profile == "dev" else "prost:bench-release")


def run(*args):
    subprocess.run(args, check=True)


# Use a disposable export container; Cargo caches live in Linux named volumes.
name = "prost-release-export-" + uuid.uuid4().hex[:10]
info = json.loads(subprocess.check_output(["docker", "image", "inspect", args.base_image], text=True))[0]
minimal = (info["Config"].get("Labels") or {}).get("io.prost.runtime") == "baseless-static"
target = args.target or ("x86_64-unknown-linux-musl" if minimal else None)
if minimal and target != "x86_64-unknown-linux-musl":
    parser.error("Baseless static requires x86_64-unknown-linux-musl binaries")
auditable = target == "x86_64-unknown-linux-musl"
builder = args.builder_image or ("prost-musl-toolchain" if auditable else "rust:1.93-bookworm")
if auditable and args.builder_image is None:
    run("docker", "build", "--target", "toolchain", "-t", builder, str(ROOT))
command = ["cargo", "auditable", "build"] if auditable else ["cargo", "build"]
command += ["--profile", args.profile, "--locked", "--workspace", "--features", args.features]
if target:
    command += ["--target", target]
try:
    run("docker", "run", "--name", name,
        "-v", f"{ROOT}:/work", "-v", "prost-target:/work/target",
        "-v", "prost-cargo:/usr/local/cargo/registry", "-w", "/work",
        "-e", "CARGO_BUILD_JOBS=2", builder, *command)
    with tempfile.TemporaryDirectory(prefix="prost-release-image-") as directory:
        context = Path(directory)
        for binary in ("prost", "prostctl", "prost-entrypoint", "prost-certd"):
            profile_dir = "debug" if args.profile == "dev" else "release"
            build_dir = f"{target}/{profile_dir}" if target else profile_dir
            run("docker", "cp", f"{name}:/work/target/{build_dir}/{binary}", str(context / binary))
        (context / "Dockerfile").write_text(
            f"FROM {args.base_image}\nCOPY prost prostctl prost-entrypoint prost-certd /usr/local/bin/\n"
            + ('LABEL io.prost.runtime="baseless-static"\n' if auditable else ''))
        run("docker", "build", "-t", tag, str(context))
finally:
    subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
