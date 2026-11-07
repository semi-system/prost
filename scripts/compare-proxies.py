"""Sequential, rotating-order 10k HTTP comparison in an isolated Compose project.

Before running, build prost:mvp and prost:bench-release. Run as root in WSL.
This script never changes the configuration or state of the regular demo project.
"""
import argparse
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "benchmarks/compose.yaml")]
TARGETS = {
    "prost-debug": (18080, "prost-bench-debug", "Prost debug MVP"),
    "prost-release": (18083, "prost-bench-release", "Prost release LTO thin"),
    "nginx": (18081, "prost-bench-nginx", "nginx official optimized binary"),
    "apache": (18082, "prost-bench-apache", "Apache httpd official optimized binary, event MPM"),
    "prost-baseline": (18084, "prost-bench-baseline", "Prost release before optimization"),
    "prost-local": (18085, "prost-bench-local", "Prost dedicated current-thread I/O workers"),
    "prost-shared": (18086, "prost-bench-shared", "Prost shared multi-thread I/O runtime"),
    "prost-pgo": (18087, "prost-bench-pgo", "Prost PGO, trained on small HTTP responses"),
    "nginx-tuned": (18089, "prost-bench-nginx-tuned", "nginx, matched 128-idle pool budget"),
    "nginx-reuseport": (18091, "prost-bench-nginx-reuseport", "nginx, 128-idle pool and reuseport"),
}


def compose(*args):
    subprocess.run([*COMPOSE, *args], check=True)


def ready(port):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=2) as r:
                if r.status == 200 and json.load(r) == {"backend": "green", "path": "/"}:
                    return
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError(f"port {port} did not become ready")


def load(port, container, label, output, backend=True, driver="asyncio", requests=10000):
    command = [sys.executable, str(ROOT / "scripts/loadtest.py"),
               "--url", f"http://127.0.0.1:{port}/", "--requests", str(requests),
               "--concurrency", "100", "--warmup", "1000",
               "--warmup-concurrency", "100", "--profile", label,
               "--driver", driver,
               "--container", container, "--output", str(output)]
    if backend:
        command += ["--container", "prost-bench-backend"]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(completed.stdout, flush=True)
    if not output.exists():
        raise RuntimeError("load test did not produce report")
    # Preserve failed rounds too; never silently drop them from the summary.
    return json.loads(output.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--keep-running", action="store_true")
    parser.add_argument("--driver", choices=["asyncio", "ab"], default="asyncio")
    parser.add_argument("--targets", nargs="+", choices=list(TARGETS), default=[name for name in TARGETS if name not in ("prost-baseline","prost-local","prost-shared","prost-pgo","nginx-tuned","nginx-reuseport")])
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--requests", type=int, default=10000)
    args = parser.parse_args()
    if args.rounds < 1 or args.requests < 1:
        parser.error("rounds and requests must be positive")
    output = args.output_dir or ROOT / ("test-results/comparison-ab" if args.driver == "ab" else "test-results/comparison")
    output.mkdir(parents=True, exist_ok=True)
    results = {name: [] for name in args.targets}
    metadata = {}
    try:
        compose("up", "-d", "backend")
        compose("stop", *TARGETS)
        ready(18090)
        direct = load(18090, "prost-bench-backend", "direct static backend", output / "backend-direct-before.json", backend=False, driver=args.driver, requests=args.requests)
        names = list(args.targets)
        for round_number in range(args.rounds):
            order = names[round_number % len(names):] + names[:round_number % len(names)]
            for name in order:
                port, container, label = TARGETS[name]
                compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", name)
                ready(port)
                if name not in metadata:
                    version_command = ["nginx", "-v"] if name.startswith("nginx") else ["httpd", "-v"] if name == "apache" else ["prost", "--version"]
                    metadata[name] = subprocess.run(["docker", "exec", container, *version_command], capture_output=True, text=True, check=True).stdout
                    if name.startswith("nginx"):
                        metadata[name] = subprocess.run(["docker", "exec", container, *version_command], capture_output=True, text=True, check=True).stderr
                report = load(port, container, label, output / f"{name}-round-{round_number + 1}.json", driver=args.driver, requests=args.requests)
                results[name].append(report)
                compose("stop", name)
        direct_after = load(18090, "prost-bench-backend", "direct static backend", output / "backend-direct-after.json", backend=False, driver=args.driver, requests=args.requests)
        summary = {"versions": metadata, "rounds": args.rounds, "driver": args.driver,
                   "direct_backend_rps": [direct["load"]["successful_requests_per_second"], direct_after["load"]["successful_requests_per_second"]],
                   "results": {}}
        for name, reports in results.items():
            container = TARGETS[name][1]
            summary["results"][name] = {
                "total_requests": sum(r["load"]["requests"] for r in reports),
                "failures": sum(r["load"]["failures"] for r in reports),
                "median_rps": statistics.median(r["load"]["successful_requests_per_second"] for r in reports),
                "median_p99_ms": statistics.median(r["load"]["latency_ms"]["p99"] for r in reports),
                "median_cpu_seconds": statistics.median(r["resources"][container]["cpu_seconds"] for r in reports),
                "median_cpu_percent": statistics.median(r["resources"][container]["average_cpu_percent_one_core_100"] for r in reports),
                "median_cpu_us_per_request": statistics.median(r["resources"][container]["cpu_microseconds_per_attempt"] for r in reports),
                "maximum_sampled_working_set_mib": max(r["resources"][container]["sampled_peak"]["working_set_bytes"] / 2**20 for r in reports),
                "maximum_sampled_anon_mib": max(r["resources"][container]["sampled_peak"]["anon_bytes"] / 2**20 for r in reports),
                "rps_by_round": [r["load"]["successful_requests_per_second"] for r in reports],
            }
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))
        if any(r["failures"] for r in summary["results"].values()) or direct["load"]["failures"] or direct_after["load"]["failures"]:
            raise SystemExit(1)
    finally:
        if not args.keep_running:
            compose("stop")


if __name__ == "__main__":
    main()
