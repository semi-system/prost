"""Launch a local Docker/WSL soak in the background, pinned to immutable images.

Writes PID, command, image IDs and expected completion time beside progress.json.
The soak owns only its UUID containers/network; stop with SIGTERM to preserve logs.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--backend-image", default="prost:bundle-backend")
    parser.add_argument("--fixture-image", default="python:3.13-alpine")
    parser.add_argument("--cert-dir", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=86400)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--pause-ms", type=int, default=20)
    parser.add_argument("--edge-policies", action="store_true")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if not (args.duration >= 10 and 1 <= args.interval <= 30 and
            1 <= args.concurrency <= 128 and args.pause_ms >= 0):
        parser.error("invalid soak duration/concurrency/rate limits")
    out = args.output_dir.resolve()
    certs = args.cert_dir.resolve()
    for filename in ("cert.pem", "key.pem"):
        if not (certs / filename).is_file():
            parser.error(f"missing TLS fixture: {certs / filename}")
    # Refuse an expiry during the campaign, including a one-hour margin.
    subprocess.run(["openssl", "x509", "-in", str(certs / "cert.pem"),
                    "-checkend", str(args.duration + 3600), "-noout"], check=True)
    images = {}
    for label, tag in (("proxy", args.image), ("backend", args.backend_image), ("fixture", args.fixture_image)):
        images[label] = subprocess.check_output(
            ["docker", "image", "inspect", "--format", "{{.Id}}", tag], text=True).strip()
    # Reserve a new directory instead of overwriting evidence from an earlier run.
    out.mkdir(parents=True, exist_ok=False)
    command = [sys.executable, str(ROOT / "scripts/soak.py"),
               "--image", images["proxy"], "--backend-image", images["backend"],
               "--fixture-image", images["fixture"],
               "--cert-dir", str(certs), "--duration", str(args.duration),
               "--interval", str(args.interval), "--concurrency", str(args.concurrency),
               "--pause-ms", str(args.pause_ms), "--output-dir", str(out)]
    if args.edge_policies:
        command.append("--edge-policies")
    source_hashes = {name: hashlib.sha256((ROOT / "scripts" / name).read_bytes()).hexdigest()
                     for name in ("soak.py", "loadtest.py")}
    started = datetime.datetime.now(datetime.timezone.utc)
    with (out / "run.log").open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.DEVNULL,
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    manifest = {
        "pid": process.pid, "command": command, "started_utc": started.isoformat(),
        "expected_end_utc": (started + datetime.timedelta(seconds=args.duration)).isoformat(),
        "image_id": images["proxy"], "backend_image_id": images["backend"],
        "fixture_image_id": images["fixture"],
        "source_sha256": source_hashes,
        "stop_command": f"kill -TERM {process.pid}",
        "note": "Launched is not passed; inspect progress.json/run.log and final summary.json."
    }
    (out / "launcher.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
