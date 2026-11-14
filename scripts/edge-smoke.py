"""Local Docker/WSL correctness/resource workload for generic edge policies.

10k mixed-size H1 requests with native Host ACL/header filters, HTTP/1 idle clock,
bounded connection window, hot apply, H2 probes and a TCP session pinned across
updates. Not a capacity benchmark. Cleans only its own UUID containers/network.
"""
import argparse
import asyncio
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
from loadtest import Container, response

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 120)
    return subprocess.run(args, check=True, **kwargs)


class Recorded:
    def __init__(self, reader):
        self.reader = reader
        self.header = b""

    def __getattr__(self, name):
        return getattr(self.reader, name)

    async def readuntil(self, separator):
        value = await self.reader.readuntil(separator)
        self.header = value
        return value


async def connect(port):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    return Recorded(reader), writer


async def get(reader, writer, path="/bytes/34", host="example.test"):
    writer.write(f"GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n".encode())
    await writer.drain()
    return await response(reader)


async def workload(port, redirect_port, tcp_port, container, ctl, out, count):
    expected = {size: (b"0123456789abcdef"*((size+15)//16))[:size] for size in (34, 65536)}
    reader, writer = await connect(port)
    assert (await get(reader, writer, host="evil.test"))[0] == 421
    assert (await get(reader, writer, host="EXAMPLE.TEST:8080"))[0] == 200
    # Longer idle budget must permit reuse beyond the 500ms header budget.
    await asyncio.sleep(.8)
    assert (await get(reader, writer))[0] == 200
    writer.close()
    await writer.wait_closed()
    reader, writer = await connect(redirect_port)
    assert (await get(reader, writer, path="/path?q=1", host="evil.test"))[0] == 421
    assert (await get(reader, writer, path="/path?q=1"))[0] == 308
    assert b"location: https://example.test/path?q=1\r\n" in reader.header.lower()
    writer.close()
    await writer.wait_closed()
    tcp, tcp_writer = await connect(tcp_port)
    status, body, _ = await get(tcp, tcp_writer)
    assert status == 200 and body == expected[34]
    control, control_writer = await connect(port)
    baseline = container.sample()
    samples = []
    stats = {"requests": 0, "errors": 0, "updates": 0, "tcp_probes": 1}
    stop = asyncio.Event()
    started = time.monotonic()

    async def worker(index):
        reader, writer = await connect(port)
        try:
            for n in range(count//32 + int(index < count%32)):
                size = (34, 65536)[(n+index)%2]
                async with asyncio.timeout(5):
                    status, body, close = await get(reader, writer, path=f"/bytes/{size}")
                    assert status == 200 and body == expected[size] and not close, (status, len(body), size)
                    assert b"x-edge: native\r\n" in reader.header.lower()
                    stats["requests"] += 1
                await asyncio.sleep(.002)
        except BaseException:
            stats["errors"] += 1
            raise
        finally:
            writer.close()
            await writer.wait_closed()

    async def updates():
        for generation in range(10):
            seed = (out/"seed.toml").read_text().replace('x-policy-generation="0"', f'x-policy-generation="{generation+1}"').replace("weight=1", f"weight={2+generation%2}")
            (out/"candidate.toml").write_text(seed)
            await asyncio.to_thread(ctl, "apply", "--file", "/cases/candidate.toml", "--if-generation", str(generation))
            assert (await get(control, control_writer))[0] == 200
            assert f"x-policy-generation: {generation+1}\r\n".encode() in control.header.lower()
            status, body, _ = await get(tcp, tcp_writer)
            assert status == 200 and body == expected[34]
            stats["tcp_probes"] += 1
            stats["updates"] += 1
            await asyncio.sleep(.05)

    async def sample():
        while not stop.is_set():
            point = await asyncio.to_thread(container.sample)
            point["rss_bytes"] = int(re.search(r"VmRSS:\s+(\d+)", Path(f"/proc/{container.pid}/status").read_text())[1])*1024
            samples.append(point)
            await asyncio.sleep(.05)

    sampler = asyncio.create_task(sample())
    try:
        await asyncio.gather(*(worker(i) for i in range(32)), updates())
    finally:
        stop.set()
        await sampler
        tcp_writer.close()
        control_writer.close()
        await tcp_writer.wait_closed()
        await control_writer.wait_closed()
    final = container.sample()
    assert stats == {"requests": count, "errors": 0, "updates": 10, "tcp_probes": 11}, stats
    elapsed = time.monotonic()-started
    # Native Host ACL is configured to explicitly allow this IP for H2 probes.
    probe = await asyncio.to_thread(run, "h2load", "-n", "1000", "-c", "8", "-m", "8", f"http://127.0.0.1:{port}/bytes/65536", capture_output=True, text=True)
    (out/"h2load.txt").write_text(probe.stdout)
    assert "1000 succeeded, 0 failed, 0 errored, 0 timeout" in probe.stdout, probe.stdout
    await asyncio.sleep(1)
    status = await asyncio.to_thread(ctl, "status")
    assert status["connections"] == 0 and status["live_generations"] == 1, status
    summary = {"image_id": container.info["Image"], "method": "32 H1 keepalive workers, alternating 34B/64KiB, Host ACL/native headers/idle clock/window enabled, 10 hot updates and pinned TCP; 2ms pacing; correctness/resource fixture, not a throughput comparison", "traffic": stats, "h2_probes": 1000, "elapsed_seconds": elapsed, "cpu_us_per_request": (final["cpu_usec"]-baseline["cpu_usec"])/count, "max_rss_mib": max(s["rss_bytes"] for s in samples)/1048576, "max_working_set_mib": max(s["working_set_bytes"] for s in samples)/1048576, "max_fds": max(s["main_process_fds"] for s in samples), "final_status": status}
    summary["containers_at_end"] = run("docker", "ps", "--format", "{{.Names}}", capture_output=True, text=True).stdout.splitlines()
    (out/"samples.json").write_text(json.dumps(samples, indent=2)+"\n")
    (out/"summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps({k:v for k,v in summary.items() if k!="final_status"}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--backend-image", default="prost:bundle-backend")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/edge-smoke")
    args = parser.parse_args()
    if args.requests < 1000:
        parser.error("use at least 1000 requests")
    out = args.output_dir.resolve()
    if (out/"summary.json").exists():
        parser.error("output directory already contains a completed result; choose a fresh directory")
    out.mkdir(parents=True, exist_ok=True)
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=2
[upstreams.web]
pool_idle_timeout_ms=1000
[[upstreams.web.endpoints]]
id="one"
address="backend:8080"
weight=1
[upstreams.stream.tcp_retry]
max_attempts=3
timeout_ms=3500
backoff_ms=25
[[upstreams.stream.endpoints]]
id="one"
address="backend:8080"
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
header_read_timeout_ms=500
keepalive_idle_timeout_ms=2000
[[listeners.plugins]]
name="host_acl"
config={ hosts=["example.test","127.0.0.1"] }
[[listeners.plugins]]
name="headers"
config={ response={ set={ x-edge="native", x-policy-generation="0" } } }
[[listeners.plugins]]
name="connection_window"
config={ max_connections=128,window_ms=10000,max_keys=4096 }
[[listeners]]
id="redirect"
address="0.0.0.0:8081"
protocol="http"
[[listeners.plugins]]
name="host_acl"
config={ hosts=["example.test"] }
[[listeners.plugins]]
name="rewrite"
config={ rules=[{action="redirect",to="https://example.test$request_uri",status=308,query="drop"}] }
[[listeners]]
id="tcp"
address="0.0.0.0:9000"
protocol="tcp"
upstream="stream"
'''
    (out/"seed.toml").write_text(seed)
    name = "prost-edge-"+uuid.uuid4().hex[:10]
    backend = name+"-backend"
    certs = ROOT/"test-results/advanced-fixtures/certs"
    created = []

    def ctl(*command):
        return json.loads(run("docker", "exec", name, "prostctl", *command, capture_output=True, text=True).stdout)["result"]

    def published(port):
        return int(json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"])

    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", args.backend_image, stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-p", "127.0.0.1::8080", "-p", "127.0.0.1::8081", "-p", "127.0.0.1::9000", args.image, stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                ctl("status")
                break
            except subprocess.CalledProcessError:
                time.sleep(.1)
        else:
            raise RuntimeError("daemon did not start")
        asyncio.run(workload(published(8080), published(8081), published(9000), Container(name), ctl, out, args.requests))
    finally:
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT, timeout=30)
            subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, timeout=30)
        subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()
