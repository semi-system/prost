"""Bounded local Docker/WSL soak: mixed H1, periodic H2/TLS, live TCP, rolling reload,
log rotation and artifact GC. Launch with --duration 86400 for the full run.
Creates and cleans only its UUID containers/network. No CI runner required.
Requires Linux, Python 3.11+, h2load, root and a local cgroup-v2 Docker daemon.
Diagnostic tools run in a separate helper, never inside the production image.
"""
import argparse
import asyncio
import datetime
import json
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid
from loadtest import Container, response

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def main():
    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f"soak interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--backend-image", default="prost:bundle-backend")
    parser.add_argument("--fixture-image", default="python:3.13-alpine")
    parser.add_argument("--cert-dir", type=Path, default=ROOT / "test-results/advanced-fixtures/certs")
    parser.add_argument("--duration", type=int, default=86400)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--pause-ms", type=int, default=20)
    parser.add_argument("--interval", type=int, default=10)
    parser.add_argument("--edge-policies", action="store_true", help="Exercise Host ACL, H1 keepalive idle, connection window and TCP dial retry")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/soak-24h")
    args = parser.parse_args()
    if not (1 <= args.concurrency <= 128 and args.duration >= 10 and 1 <= args.interval <= 30 and args.pause_ms >= 0):
        parser.error("invalid soak duration/concurrency/rate limits")
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    if (out / "progress.json").exists():
        parser.error("output directory already contains a soak run; use a fresh directory")
    certs = args.cert_dir.resolve()
    name = "prost-soak-" + uuid.uuid4().hex[:10]
    backend = name + "-backend"
    monitor = name + "-monitor"
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
metrics_address="127.0.0.1:9090"
io_workers=2
reload_mode="rolling"
drain_seconds=10
observe_plugins=true
[runtime.access_log]
path="/var/lib/prost/access.jsonl"
format="compact"
sample_every=1000
queue_capacity=256
exclude_paths=["/ready"]
[upstreams.web]
observe_pool_state=true
observe_connection_reuse=true
pool_idle_timeout_ms=1000
[upstreams.web.http.cache]
max_bytes=16777216
max_entries=128
max_object_bytes=262144
max_ttl_seconds=30
[upstreams.web.http.compression]
algorithms=["gzip","br"]
content_types=["application/octet-stream"]
max_concurrent=8
[upstreams.web.http.retry]
max_attempts=2
timeout_ms=2000
[[upstreams.web.endpoints]]
id="one"
address="backend:8080"
weight=1
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
[[listeners.plugins]]
name="headers"
config={ response={ set={ x-soak-generation="GEN" } } }
[listeners.http2_settings]
header_read_timeout_ms=5000
stream_write_timeout_ms=5000
[[listeners]]
id="https"
address="0.0.0.0:8443"
protocol="http"
upstream="web"
[listeners.tls]
certificate="/certs/cert.pem"
private_key="/certs/key.pem"
[listeners.http2_settings]
header_read_timeout_ms=5000
stream_write_timeout_ms=5000
[[listeners]]
id="tcp"
address="0.0.0.0:9000"
protocol="tcp"
upstream="web"
'''
    if args.edge_policies:
        seed = seed.replace('[upstreams.web.http.cache]', '''[upstreams.web.tcp_retry]
max_attempts=3
timeout_ms=3000
backoff_ms=10
[upstreams.web.http.cache]
''')
        seed = seed.replace('protocol="http"\nupstream="web"', '''protocol="http"
upstream="web"
header_read_timeout_ms=1000
keepalive_idle_timeout_ms=30000
[[listeners.plugins]]
name="host_acl"
config={hosts=["soak.local","127.0.0.1"]}
'''.strip())
        seed = seed.replace('protocol="tcp"\nupstream="web"', '''protocol="tcp"
upstream="web"
[[listeners.plugins]]
name="connection_window"
config={max_connections=1000,window_ms=1000,max_keys=1024,expire_ms=30000}
'''.strip())
    (out / "seed.toml").write_text(seed.replace('"GEN"', '"0"'))
    created = []
    manifest = {"name": name, "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(), "duration_seconds": args.duration, "concurrency": args.concurrency, "pause_ms": args.pause_ms, "edge_policies": args.edge_policies, "state": "starting"}
    save(out / "progress.json", manifest)

    def ctl(*command):
        return json.loads(run("docker", "exec", name, "prostctl", *command, capture_output=True, text=True).stdout)["result"]

    def metrics():
        return run("docker", "exec", monitor, "python", "-c",
                   "import urllib.request,sys; sys.stdout.write(urllib.request.urlopen('http://127.0.0.1:9090/metrics',timeout=5).read().decode())",
                   capture_output=True, text=True).stdout

    def rotate_log():
        # The production image is shellless. Both paths are in its data volume;
        # the helper uses the same UID and atomically replaces one rotated file.
        run("docker", "exec", monitor, "python", "-c",
            "import os; os.replace('/var/lib/prost/access.jsonl','/var/lib/prost/access.jsonl.1')")

    def resources():
        sample = Container(name)
        data = sample.sample()
        raw = Path(f"/proc/{sample.pid}/status").read_text()
        data["rss_bytes"] = int(re.search(r"VmRSS:\s+(\d+)", raw)[1]) * 1024
        data["status"] = ctl("status")
        return data

    def published(port):
        info = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]
        return int(info["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"])

    async def workload(http_port, tcp_port, https_port):
        stop = asyncio.Event()
        stats = {"requests": 0, "response_body_bytes": 0, "errors": 0, "updates": 0, "h2_probes": 0, "tcp_probes": 0}
        if args.edge_policies:
            stats.update({"host_denials": 0, "idle_reuses": 0})
        start = time.monotonic()
        peaks = {"rss_bytes": 0, "fds": 0, "live_generations": 0}
        failure = None

        async def worker(index):
            reader = writer = None
            count = index
            try:
                while not stop.is_set():
                    path, size = [("/bytes/34", 34), ("/bytes/65536", 65536), ("/cached/small", 65536), ("/stream", 262144)][count % 4]
                    try:
                        async with asyncio.timeout(10):
                            if writer is None:
                                reader, writer = await asyncio.open_connection("127.0.0.1", http_port)
                            # A rolling reload may gracefully close an idle H1
                            # keepalive connection. Use a fresh connection for
                            # each load request so such closes are not retried
                            # or misreported as lost in-flight requests.
                            writer.write(f"GET {path} HTTP/1.1\r\nHost: soak.local\r\nConnection: close\r\n\r\n".encode())
                            await writer.drain()
                            status, body, close = await response(reader)
                            assert status == 200 and len(body) == size
                            assert body == (b"0123456789abcdef" * ((size + 15)//16))[:size]
                            stats["requests"] += 1
                            stats["response_body_bytes"] += len(body)
                            assert close, "HTTP/1 close was not honored"
                            writer.close()
                            await writer.wait_closed()
                            writer = None
                        await asyncio.sleep(args.pause_ms / 1000)
                        count += 1
                    except Exception:
                        stats["errors"] += 1
                        stop.set()
                        raise
            finally:
                if writer:
                    writer.close()
                    await writer.wait_closed()

        async def edge_probe(idle=False):
            reader, writer = await asyncio.open_connection("127.0.0.1", http_port)
            try:
                if idle:
                    for attempt in range(2):
                        writer.write(b"GET /bytes/34 HTTP/1.1\r\nHost: soak.local\r\n\r\n")
                        await writer.drain()
                        status, body, close = await asyncio.wait_for(response(reader), 5)
                        assert status == 200 and len(body) == 34 and not close
                        if attempt == 0:
                            await asyncio.sleep(1.5)  # Past the 1s header budget.
                    stats["idle_reuses"] += 1
                writer.write(b"GET /bytes/34 HTTP/1.1\r\nHost: unknown.invalid\r\n\r\n")
                await writer.drain()
                status, _, _ = await asyncio.wait_for(response(reader), 5)
                assert status == 421, f"Host ACL returned {status}"
                stats["host_denials"] += 1
            finally:
                writer.close()
                await writer.wait_closed()

        if args.edge_policies:
            await edge_probe(idle=True)
        workers = [asyncio.create_task(worker(i)) for i in range(args.concurrency)]
        tcp_writer = None
        try:
            with (out / "samples.jsonl").open("w") as records:
                cycle = 0
                while time.monotonic() - start < args.duration and not stop.is_set():
                    wait = min(args.interval, args.duration - (time.monotonic() - start))
                    await asyncio.sleep(max(0, wait))
                    for worker_task in workers:
                        if worker_task.done():
                            worker_task.result()
                    generation = stats["updates"]
                    (out / "candidate.toml").write_text(seed.replace('"GEN"', f'"{generation+1}"').replace("weight=1", f"weight={1+(generation%2)}"))
                    # Keep a TCP session alive across one commit, then release
                    # it before the drain deadline and wait for the old worker.
                    # Holding it across every reload would deliberately force
                    # termination under the rolling lifecycle being tested.
                    tcp_reader, tcp_writer = await asyncio.open_connection("127.0.0.1", tcp_port)

                    async def tcp_probe():
                        tcp_writer.write(b"GET /bytes/34 HTTP/1.1\r\nHost: soak.local\r\n\r\n")
                        await tcp_writer.drain()
                        status, body, close = await asyncio.wait_for(response(tcp_reader), 5)
                        assert status == 200 and len(body) == 34 and not close
                        stats["tcp_probes"] += 1

                    await tcp_probe()
                    result = await asyncio.to_thread(ctl, "reload", "--file", "/cases/candidate.toml", "--if-generation", str(generation))
                    assert result["generation"] == generation + 1, result
                    stats["updates"] += 1
                    await tcp_probe()
                    tcp_writer.close()
                    await tcp_writer.wait_closed()
                    tcp_writer = None
                    drained = await asyncio.to_thread(ctl, "workers", "wait", "--generation", str(generation+1), "--timeout", "20")
                    assert drained["drained"] is True, drained
                    if args.edge_policies:
                        await edge_probe()
                    # Bounded H2/TLS probes also exercise compression negotiation.
                    probe = await asyncio.to_thread(run, "h2load", "-n", "100", "-c", "4", "-m", "4", "-H", "accept-encoding: gzip", f"https://127.0.0.1:{https_port}/bytes/65536", capture_output=True, text=True)
                    assert "100 succeeded, 0 failed, 0 errored, 0 timeout" in probe.stdout, probe.stdout
                    stats["h2_probes"] += 100
                    await asyncio.to_thread(ctl, "access-log", "flush", "--if-generation", str(generation+1))
                    # Keep one rotated file, so a day-long run has bounded log disk use.
                    await asyncio.to_thread(rotate_log)
                    await asyncio.to_thread(ctl, "access-log", "reopen", "--if-generation", str(generation+1))
                    if cycle % 6 == 0:
                        await asyncio.to_thread(ctl, "artifacts", "gc", "--delete", "--min-age-seconds", "0", "--if-generation", str(generation+1))
                    sample = await asyncio.to_thread(resources)
                    elapsed = time.monotonic() - start
                    sample.update({"elapsed_seconds": elapsed, "traffic": stats.copy()})
                    records.write(json.dumps(sample) + "\n")
                    records.flush()
                    peaks["rss_bytes"] = max(peaks["rss_bytes"], sample["rss_bytes"])
                    peaks["fds"] = max(peaks["fds"], sample["main_process_fds"])
                    peaks["live_generations"] = max(peaks["live_generations"], sample["status"]["live_generations"])
                    assert sample["rss_bytes"] < 192*1024*1024, "RSS budget exceeded"
                    assert sample["main_process_fds"] < 512 and sample["tasks"] < 512, "FD/task budget exceeded"
                    assert sample["status"]["live_generations"] == 1, "old generation accumulation after drain"
                    assert sample["status"]["worker_generations"]["retired_total"] == stats["updates"], "worker retirement did not complete"
                    assert sample["status"]["worker_generations"]["forced_total"] == 0, "unexpected forced drain"
                    assert sample["status"]["access_log"]["writer_alive"] == 1
                    assert sample["status"]["access_log"]["io_errors"] == 0 and sample["status"]["access_log"]["dropped_queue_full"] == 0, "unexpected access log loss"
                    manifest.update({"state": "running", "elapsed_seconds": elapsed, "traffic": stats.copy(), "latest_resources": sample})
                    save(out / "progress.json", manifest)
                    cycle += 1
        except BaseException as error:
            failure = error
            raise
        finally:
            stop.set()
            if tcp_writer:
                tcp_writer.close()
                await tcp_writer.wait_closed()
            await asyncio.gather(*workers, return_exceptions=True)
            await asyncio.sleep(2)
            final = await asyncio.to_thread(resources)
            final_metrics=await asyncio.to_thread(metrics)
            (out / "final-metrics.prom").write_text(final_metrics)
            if failure is None:
                assert final["status"]["live_generations"] == 1 and final["status"]["connections"] == 0, final
                assert stats["requests"] > 0 and stats["errors"] == 0, stats
                cache_hits=re.search(r'prost_http_cache_hits_total\{upstream="web"\} (\d+)',final_metrics)
                assert cache_hits and int(cache_hits[1])>0, "cache was not exercised"
                assert time.monotonic() - start >= args.duration
                manifest.update({"state": "passed", "elapsed_seconds": time.monotonic()-start, "traffic": stats.copy(), "final": final, "max_rss_mib": peaks["rss_bytes"]/1048576, "max_fds": peaks["fds"], "max_live_generations": peaks["live_generations"]})
                save(out / "summary.json", manifest)
                save(out / "progress.json", manifest)
                print(json.dumps({k:manifest[k] for k in ("state","elapsed_seconds","traffic","max_rss_mib","max_fds","max_live_generations")},indent=2),flush=True)

    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", args.backend_image, stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-v", f"{certs}:/certs:ro", "-p", "127.0.0.1::8080", "-p", "127.0.0.1::8443", "-p", "127.0.0.1::9000", args.image, stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                ctl("status")
                break
            except subprocess.CalledProcessError:
                time.sleep(.1)
        else:
            raise RuntimeError("proxy did not become ready")
        manifest["image_id"] = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["Image"]
        created.append(monitor)
        run("docker", "run", "-d", "--name", monitor, "--network", "container:"+name,
            "--volumes-from", name, "--user", "10001:10001", "--memory", "64m", "--cpus", "0.25",
            args.fixture_image, "python", "-c", "import signal; signal.pause()", stdout=subprocess.DEVNULL)
        manifest["fixture_image_id"] = json.loads(run("docker", "inspect", monitor, capture_output=True, text=True).stdout)[0]["Image"]
        manifest["backend_image_id"] = json.loads(run("docker", "inspect", backend, capture_output=True, text=True).stdout)[0]["Image"]
        asyncio.run(workload(published(8080), published(9000), published(8443)))
    except BaseException as error:
        manifest.update({"state":"failed","error":repr(error)})
        save(out / "progress.json", manifest)
        raise
    finally:
        for container in reversed(created):
            subprocess.run(["docker","logs",container],stdout=(out / f"{container}.log").open("w"),stderr=subprocess.STDOUT)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
