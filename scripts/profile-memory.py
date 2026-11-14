"""Matched HTTP/TLS/slow-stream memory stages in an isolated Docker/WSL project.

Optional heaptrack traces allocations; never use its timing/RSS as production
performance measurements. Use --heap only with benchmarks/profiling/Dockerfile.
"""
import argparse
import asyncio
import json
from pathlib import Path
import re
import ssl
import subprocess
import time
from loadtest import Container

ROOT = Path(__file__).resolve().parent.parent
NAME = "prost-memory-profile"


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def ctl(*args):
    return json.loads(run("docker", "exec", NAME, "prostctl", *args, capture_output=True, text=True).stdout)


async def scenario(args, output):
    sample = Container(NAME)
    rows = run("docker", "top", NAME, "-eo", "pid,comm", capture_output=True, text=True).stdout.splitlines()
    pid = next(int(line.split()[0]) for line in rows if line.split()[-1] == "prost")
    stages = []
    def capture(stage):
        exposition = run("nsenter", "--target", str(sample.pid), "--net", "curl", "-fsS", "--max-time", "3", "http://127.0.0.1:9090/metrics", capture_output=True).stdout
        (output / f"metrics-{stage}.prom").write_bytes(exposition)
        fields = {}
        for line in Path(f"/proc/{pid}/smaps_rollup").read_text().splitlines():
            if ":" in line:
                k, v = line.split(":", 1)
                if v.strip().endswith("kB"): fields[k] = int(v.split()[0]) * 1024
        stages.append({"stage": stage, "monotonic": time.monotonic(), "process": fields,
                       "fds": len(list(Path(f"/proc/{pid}/fd").iterdir())),
                       "threads": len(list(Path(f"/proc/{pid}/task").iterdir())),
                       "cgroup": sample.sample(), "status": ctl("status")["result"]})
        print(stage, "RSS MiB", round(fields["Rss"] / 1048576, 2), flush=True)

    capture("startup")
    # Request timing is intentionally not reported by this memory harness.
    for port, count in [(19881, args.requests), (19844, max(256, args.requests // 10))]:
        protocol = "http" if port == 19881 else "https"
        raw = run("h2load", "--h1", "-n", str(count), "-c", str(min(args.clients, count)), "-t", "2",
                  f"{protocol}://127.0.0.1:{port}/bytes/34", capture_output=True, text=True).stdout
        (output / f"load-{protocol}.txt").write_text(raw)
        assert re.search(rf"{count} succeeded, 0 failed, 0 errored, 0 timeout", raw), raw
    capture("after_load")
    idle = []
    streams = []
    context = ssl.create_default_context(cafile=str(ROOT / "test-results/advanced-fixtures/certs/cert.pem"))
    context.set_alpn_protocols(["http/1.1"])
    async def connect(port, path, encrypted=False):
        reader, writer = await asyncio.wait_for(asyncio.open_connection(
            "127.0.0.1", port, ssl=context if encrypted else None,
            server_hostname="localhost" if encrypted else None, limit=32768), 15)
        writer.write(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
        await writer.drain()
        headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 15)
        assert b" 200 " in headers.split(b"\r\n")[0]
        return reader, writer
    try:
        async def open_idle():
            r, w = await connect(19881, "/bytes/34")
            assert await r.readexactly(34) == (b"0123456789abcdef" * 3)[:34]
            idle.append((r, w))
        await asyncio.gather(*(open_idle() for _ in range(args.clients)))
        capture("idle_clients")
        async def open_stream():
            r, w = await connect(19844, "/bytes/16777216", True)
            assert await r.readexactly(4096) == b"0123456789abcdef" * 256
            streams.append((r, w))
        await asyncio.gather(*(open_stream() for _ in range(args.streams)))
        await asyncio.sleep(1)
        capture("slow_tls_streams")
        candidate = output / "candidate.toml"
        config = (output / "seed.toml").read_text()
        for i in range(10):
            candidate.write_text(config.replace("weight = 1", f"weight = {1 + i % 2}"))
            ctl("apply", "--file", "/profiles/candidate.toml", "--if-generation", str(i))
        capture("after_10_updates")
    finally:
        for _, writer in streams + idle:
            writer.transport.abort()
        streams.clear(); idle.clear()
    await asyncio.sleep(1)
    capture("after_cancel")
    await asyncio.sleep(2)
    capture("after_pool_expiry")
    assert stages[-1]["status"]["live_generations"] == 1
    assert stages[-1]["status"]["connections"] == 0
    return stages


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--image", default="prost:native")
    p.add_argument("--heap", action="store_true")
    p.add_argument("--http-limits", action="store_true", help="Enable streaming body limits and granular HTTP/socket timeouts")
    p.add_argument("--env", action="append", default=[], help="Explicit container environment, e.g. MALLOC_ARENA_MAX=2")
    p.add_argument("--tls-buffer-limit", type=int, help="Only supported by images with TLS buffer_limit_bytes")
    p.add_argument("--requests", type=int, default=10000)
    p.add_argument("--clients", type=int, default=128)
    p.add_argument("--streams", type=int, default=32)
    p.add_argument("--output-dir", type=Path, required=True)
    args = p.parse_args()
    if min(args.requests, args.clients, args.streams) < 1: p.error("counts must be positive")
    output = args.output_dir.resolve(); output.mkdir(parents=True, exist_ok=True)
    certs = ROOT / "test-results/advanced-fixtures/certs"
    config = '''schema_version = 1
[runtime]
admin_socket = "/run/prost/admin.sock"
artifact_dir = "/var/lib/prost/artifacts"
metrics_address = "127.0.0.1:9090"
io_workers = 2
drain_seconds = 5
[upstreams.main]
pool_max_idle_per_host = 128
pool_idle_timeout_ms = 1000
[[upstreams.main.endpoints]]
id = "one"
address = "backend:8080"
weight = 1
[[listeners]]
id = "plain"
address = "0.0.0.0:8080"
protocol = "http"
upstream = "main"
[[listeners]]
id = "tls"
address = "0.0.0.0:8443"
protocol = "http"
upstream = "main"
[listeners.tls]
certificate = "/certs/cert.pem"
private_key = "/certs/key.pem"
'''
    if args.tls_buffer_limit is not None: config += f"buffer_limit_bytes = {args.tls_buffer_limit}\n"
    if args.http_limits:
        import importlib.util
        spec = importlib.util.spec_from_file_location("limits_config", ROOT / "scripts/prepare-limits-bench.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        config = module.enable_limits(config)
    (output / "seed.toml").write_text(config)
    run("docker", "network", "create", NAME, stdout=subprocess.DEVNULL)
    metadata = {"image": args.image, "heaptrack": args.heap, "requests": args.requests,
                "environment": args.env,
                "tls_buffer_limit": args.tls_buffer_limit, "http_limits": args.http_limits, "metrics_scrapes": True,
                "idle_clients": args.clients, "slow_tls_streams": args.streams,
                "other_containers": run("docker", "ps", "--format", "{{.Names}}", capture_output=True, text=True).stdout.splitlines()}
    try:
        run("docker", "run", "-d", "--name", NAME + "-backend", "--network", NAME, "--network-alias", "backend",
            "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", "prost:advanced-backend", stdout=subprocess.DEVNULL)
        command = ["docker", "run", "-d", "--name", NAME, "--network", NAME, "--cpus", "2", "--memory", "512m",
                   "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{output}:/profiles", "-v", f"{output/'seed.toml'}:/etc/prost/seed.toml:ro",
                   "-v", f"{certs}:/certs:ro", "-p", "127.0.0.1:19881:8080", "-p", "127.0.0.1:19844:8443"]
        for env in args.env: command += ["-e", env]
        command.append(args.image)
        if args.heap: command += ["heaptrack", "-o", "/profiles/heap", "prost", "run", "--config", "/var/lib/prost/prost.toml"]
        run(*command, stdout=subprocess.DEVNULL)
        for i in range(100):
            result = subprocess.run(["docker", "exec", NAME, "prostctl", "status"], capture_output=True)
            if result.returncode == 0: break
            time.sleep(.1)
        else: raise RuntimeError("proxy did not become ready")
        metadata["image_id"] = json.loads(run("docker", "inspect", NAME, capture_output=True, text=True).stdout)[0]["Image"]
        stages = asyncio.run(scenario(args, output))
        (output / "summary.json").write_text(json.dumps({"metadata": metadata, "stages": stages}, indent=2) + "\n")
        if args.heap:
            run("docker", "exec", NAME, "pkill", "-TERM", "-x", "prost")
            run("docker", "wait", NAME, timeout=20, stdout=subprocess.DEVNULL)
            trace = next(output.glob("heap*.gz"), None) or next(output.glob("heap*.zst"))
            with (output / "heap-report.txt").open("w") as stream:
                run("docker", "run", "--rm", "--entrypoint", "heaptrack_print", "-v", f"{output}:/profiles", args.image,
                    "-f", "/profiles/" + trace.name, "-n", "15", "-s", "3", "-M", "/profiles/massif.txt", stdout=stream, stderr=subprocess.STDOUT)
    finally:
        for container in (NAME, NAME + "-backend"):
            subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", NAME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__": main()
