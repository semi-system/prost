"""Local Docker/WSL correctness and resource run: PROXY v2 before backend mTLS.

Uses an independent Go backend decoder, 16 client identities and 10 live updates.
This is not a throughput benchmark. Creates/cleans only its UUID containers.
"""
import argparse
import asyncio
import json
from pathlib import Path
import re
import ssl
import subprocess
import time
import uuid
import warnings
from loadtest import Container, response

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 120)
    return subprocess.run(args, check=True, **kwargs)


async def workload(port, context, ctl, container, out, requests):
    async def connect(index, ctx=context, name="localhost"):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            writer.write(f"PROXY TCP4 198.51.100.{index+1} 203.0.113.10 {10000+index} 10443\r\n".encode())
            await writer.drain()
            await writer.start_tls(ctx, server_hostname=name, ssl_handshake_timeout=3)
            return reader, writer
        except BaseException:
            writer.close()
            try:
                await writer.wait_closed()
            except (ssl.SSLError, ConnectionError):
                pass
            raise

    checks = []
    reader, writer = await connect(102)
    writer.write(b"GET /identity HTTP/1.1\r\nHost: localhost\r\n\r\n")
    await writer.drain()
    status, body, _ = await response(reader)
    assert status == 200 and body == b"198.51.100.103:10102", (status, body)
    writer.close()
    await writer.wait_closed()
    checks.append("positive verified TLS/mTLS/PROXY control")
    # OpenSSL sends real legacy/CBC/DHE ClientHellos; do not count a local
    # NO_CIPHERS/NO_PROTOCOLS error as evidence of server rejection.
    for label, version, ciphers in [
        ("TLS1.0", ssl.TLSVersion.TLSv1, "ALL:@SECLEVEL=0"),
        ("TLS1.1", ssl.TLSVersion.TLSv1_1, "ALL:@SECLEVEL=0"),
        ("CBC", ssl.TLSVersion.TLSv1_2, "ECDHE-RSA-AES128-SHA256:@SECLEVEL=0"),
        ("DHE", ssl.TLSVersion.TLSv1_2, "DHE-RSA-AES128-GCM-SHA256:@SECLEVEL=0"),
    ]:
        ctx = ssl.create_default_context(cafile=str(out / "certs/ca.pem"))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            ctx.minimum_version = ctx.maximum_version = version
        # Negative TLS fixture intentionally offers forbidden protocols/ciphers and asserts rejection; set_ciphers follows immediately.
        # nosemgrep: python.lang.security.audit.insecure-transport.ssl.no-set-ciphers.no-set-ciphers
        ctx.set_ciphers(ciphers)
        try:
            _, writer = await connect(100, ctx)
        except ssl.SSLError as error:
            assert error.reason in ("TLSV1_ALERT_PROTOCOL_VERSION", "SSLV3_ALERT_HANDSHAKE_FAILURE"), error
            checks.append(f"{label} handshake rejected: {error.reason}")
        except (ConnectionError, asyncio.TimeoutError) as error:
            raise AssertionError(f"{label}: expected explicit TLS rejection, got {error!r}") from error
        else:
            writer.close()
            await writer.wait_closed()
            raise AssertionError(f"{label} was accepted")

    for name in ["unknown.test", None]:
        ctx = ssl.create_default_context(cafile=str(out / "certs/ca.pem"))
        ctx.check_hostname = False
        try:
            _, writer = await connect(101, ctx, name)
        except (ssl.SSLError, ConnectionError):
            checks.append(f"strict SNI rejected {name!r}")
        else:
            writer.close()
            await writer.wait_closed()
            raise AssertionError(f"unexpected SNI accepted: {name!r}")

    samples, stats = [], {"requests": 0, "errors": 0, "updates": 0}
    start = time.monotonic()
    baseline = container.sample()
    stop = asyncio.Event()

    async def worker(index):
        reader = writer = None
        try:
            reader, writer = await connect(index)
            expected = f"198.51.100.{index+1}:{10000+index}".encode()
            for _ in range(requests // 16 + int(index < requests % 16)):
                async with asyncio.timeout(5):
                    writer.write(b"GET /identity HTTP/1.1\r\nHost: localhost\r\n\r\n")
                    await writer.drain()
                    status, body, close = await response(reader)
                    assert status == 200 and body == expected and not close, (status, body, expected)
                    stats["requests"] += 1
                await asyncio.sleep(.002)
        except BaseException:
            stats["errors"] += 1
            raise
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()

    async def sample():
        while not stop.is_set():
            point = await asyncio.to_thread(container.sample)
            point["rss_bytes"] = int(re.search(r"VmRSS:\s+(\d+)", Path(f"/proc/{container.pid}/status").read_text())[1]) * 1024
            samples.append(point)
            await asyncio.sleep(.05)

    async def updates():
        for generation in range(10):
            config = (out / "seed.toml").read_text().replace("weight=1", f"weight={2+generation%2}")
            (out / "candidate.toml").write_text(config)
            await asyncio.to_thread(ctl, "apply", "--file", "/cases/candidate.toml", "--if-generation", str(generation))
            stats["updates"] += 1
            await asyncio.sleep(.05)

    sampler = asyncio.create_task(sample())
    try:
        await asyncio.gather(*(worker(i) for i in range(16)), updates())
    finally:
        stop.set()
        await sampler
    final = container.sample()
    assert stats["requests"] == requests and stats["errors"] == 0 and stats["updates"] == 10, stats
    elapsed = time.monotonic() - start
    await asyncio.sleep(1)
    status = await asyncio.to_thread(ctl, "status")
    assert status["connections"] == 0 and status["live_generations"] == 1, status
    summary = {"image_id": container.info["Image"], "method": "16 HTTPS keepalive clients, independent Go PROXY-v2/mTLS backend, 10 hot updates, deliberate 2ms request pacing; correctness/resource fixture, not capacity benchmark", "elapsed_seconds": elapsed, "traffic": stats, "checks": checks, "cpu_us_per_request": (final["cpu_usec"] - baseline["cpu_usec"]) / requests, "max_rss_mib": max(p["rss_bytes"] for p in samples)/1048576, "max_working_set_mib": max(p["working_set_bytes"] for p in samples)/1048576, "max_fds": max(p["main_process_fds"] for p in samples), "final_status": status}
    (out / "samples.json").write_text(json.dumps(samples, indent=2)+"\n")
    (out / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps({k:v for k,v in summary.items() if k != "final_status"}, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--backend-image", default="prost:rusty-backend")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/rusty-smoke")
    args = parser.parse_args()
    if args.requests < 1000:
        parser.error("use at least 1000 requests")
    out = args.output_dir.resolve()
    certs = out / "certs"
    certs.mkdir(parents=True, exist_ok=True)
    quiet = {"stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "2", "-nodes", "-subj", "/CN=Prost fixture CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", str(certs/"ca.key"), "-out", str(certs/"ca.pem"), **quiet)
    run("openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost", "-keyout", str(certs/"key.pem"), "-out", str(certs/"leaf.csr"), **quiet)
    (certs/"leaf.ext").write_text("basicConstraints=critical,CA:FALSE\nsubjectAltName=DNS:localhost\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=critical,digitalSignature,keyEncipherment\n")
    run("openssl", "x509", "-req", "-in", str(certs/"leaf.csr"), "-CA", str(certs/"ca.pem"), "-CAkey", str(certs/"ca.key"), "-CAcreateserial", "-sha256", "-days", "2", "-extfile", str(certs/"leaf.ext"), "-out", str(certs/"cert.pem"), **quiet)
    # Newly generated disposable fixture keys only, for non-root Docker fixtures.
    for path in certs.iterdir():
        path.chmod(0o644)
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
max_connections=256
max_requests=256
max_proxy_connections=64
io_workers=2
drain_seconds=2
[upstreams.web]
send_proxy_v2=true
health_interval_ms=1000
pool_idle_timeout_ms=5000
[upstreams.web.health_check]
protocol="http"
path="/health"
host="localhost"
timeout_ms=2000
[upstreams.web.tls]
server_name="localhost"
ca_certificate="/certs/ca.pem"
[upstreams.web.tls.identity]
certificate="/certs/cert.pem"
private_key="/certs/key.pem"
[upstreams.web.tls.policy]
min_version="tls12"
session_resumption=false
[[upstreams.web.endpoints]]
id="one"
address="backend:8443"
weight=1
[[listeners]]
id="https"
address="0.0.0.0:8443"
protocol="http"
upstream="web"
max_connections_per_ip=4
tls_handshake_timeout_ms=2000
[listeners.proxy_protocol]
trusted_peers=["127.0.0.1/32","172.16.0.0/12"]
timeout_ms=1000
[listeners.tls]
certificate="/certs/cert.pem"
private_key="/certs/key.pem"
reject_unknown_sni=true
[[listeners.tls.certificates]]
server_names=["localhost"]
certificate="/certs/cert.pem"
private_key="/certs/key.pem"
[listeners.tls.policy]
min_version="tls12"
session_resumption=false
'''
    (out / "seed.toml").write_text(seed)
    name = "prost-rusty-" + uuid.uuid4().hex[:10]
    backend = name + "-backend"
    created = []

    def ctl(*command):
        return json.loads(run("docker", "exec", name, "prostctl", *command, capture_output=True, text=True).stdout)["result"]

    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", args.backend_image, stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-v", f"{certs}:/certs:ro", "-p", "127.0.0.1::8443", args.image, stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                if ctl("status")["upstreams"]["web"][0]["healthy"]:
                    break
            except subprocess.CalledProcessError:
                pass
            time.sleep(.1)
        else:
            raise RuntimeError("daemon/backend did not become ready")
        port = int(json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["NetworkSettings"]["Ports"]["8443/tcp"][0]["HostPort"])
        context = ssl.create_default_context(cafile=str(certs/"ca.pem"))
        asyncio.run(workload(port, context, ctl, Container(name), out, args.requests))
    finally:
        for container in reversed(created):
            with (out / f"{container}.log").open("w") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT, timeout=30)
            subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, timeout=30)
        subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()
