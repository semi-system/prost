"""Isolated seven-port mail-edge soak with real Pebble HTTP-01 issuance/renewal.

Requires Docker on local Linux, root for cgroup sampling, openssl and h2load.
The SMTP/IMAP fixture negotiates STARTTLS and IDLE, without storing/delivering
mail. Never connects to existing application containers. SIGTERM saves evidence
and cleans only the containers/network created by this invocation.
"""
import argparse
import asyncio
import datetime
import hashlib
import json
import os
from pathlib import Path
import signal
import shutil
import ssl
import subprocess
import time
import tempfile
import uuid
from loadtest import Container, response

ROOT = Path(__file__).resolve().parent.parent
PEBBLE = "ghcr.io/letsencrypt/pebble@sha256:0c81f05db371f436c97fb894bd6d2ab8fc9c8e2ce40f8bef8a8e15941bcd75ef"


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:mail-next")
    parser.add_argument("--fixture-image", default="python:3.13-alpine")
    parser.add_argument("--backend-image", help="Optional compiled mail-stress fixture with reload/ACK verification")
    parser.add_argument("--dns01", action="store_true", help="Exercise wildcard issuance with authenticated HTTPS DNS hooks")
    parser.add_argument("--idle-silence-seconds", type=int, default=360)
    parser.add_argument("--stress", action="store_true", help="Run sustained HTTP/2 upload/download load beside mail checks")
    parser.add_argument("--stress-connections", type=int, default=128)
    parser.add_argument("--stress-streams", type=int, default=8)
    parser.add_argument("--stress-round-seconds", type=int, default=20)
    parser.add_argument("--duration", type=int, default=240)
    parser.add_argument("--validity", type=int, default=180)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--interval", type=int, default=5)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/mail-edge-soak")
    args = parser.parse_args()
    if args.stress and (not args.backend_image or not 1 <= args.stress_connections <= 1024 or not 1 <= args.stress_streams <= 32 or not 5 <= args.stress_round_seconds <= 60):
        parser.error("stress needs compiled backend, connections 1..1024, streams 1..32, rounds 5..60s")
    if not (180 <= args.duration and 180 <= args.validity <= 86400 and 1 <= args.clients <= 32 and 1 <= args.interval <= 30):
        parser.error("duration/validity >=180s, validity <=86400s, clients 1..32, interval 1..30s")
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="prost-mail-runtime-"))
    # Linux-native state is required for ownership, private-file modes, flock
    # and fsync; WSL /mnt/c or /mnt/d bind mounts may not implement them.
    work.chmod(0o755)
    name = "prost-mail-" + uuid.uuid4().hex[:10]
    names = {role: name + "-" + role for role in ("edge", "certd", "ca", "backend", "dns")}
    created = []
    report = {"state": "starting", "method": f"isolated protocol fixture; real {'DNS-01' if args.dns01 else 'HTTP-01'}, STARTTLS and IDLE; not mail delivery/storage qualification", "errors": 0}
    started = time.monotonic()
    network = False
    save(out / "launcher.json", {"image": args.image, "fixture_image": args.fixture_image,
         "containers": names, "network": name, "duration_seconds": args.duration,
         "certificate_validity_seconds": args.validity, "runtime_directory": str(work),
         "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})

    def interrupted(signum, _):
        raise KeyboardInterrupt(f"interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted)

    def inspect(container):
        return json.loads(run("docker", "inspect", container, capture_output=True, text=True).stdout)[0]

    def port(container, target):
        return int(inspect(container)["NetworkSettings"]["Ports"][f"{target}/tcp"][0]["HostPort"])

    def ctl(*command):
        return json.loads(run("docker", "exec", names["edge"], "prostctl", *command, capture_output=True, text=True).stdout)["result"]

    def launch(role, image, command, mounts=(), ports=(), aliases=(), extra=()):
        cmd = ["docker", "run", "-d", "--name", names[role], "--network", name,
               "--user", "10001:10001", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
               "--ulimit", "nofile=65536:65536",
               "--memory", "512m" if args.stress else "256m", "--cpus", "2", "--log-opt", "max-size=10m", "--log-opt", "max-file=3"]
        for alias in aliases:
            cmd += ["--network-alias", alias]
        for mount in mounts:
            cmd += ["-v", mount]
        for target in ports:
            cmd += ["-p", f"127.0.0.1::{target}"]
        run(*cmd, *extra, image, *command, capture_output=True)
        created.append(names[role])

    try:
        for directory in ("state", "certd-state", "certificates", "socket"):
            path = work / directory
            path.mkdir(mode=0o700)
            os.chown(path, 10001, 10001)
        (work / "cases").mkdir()
        cases = work / "cases"
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "3", "-subj", "/CN=ca.test",
            "-addext", "subjectAltName=DNS:ca.test,DNS:backend.test,DNS:dns.test", "-addext", "basicConstraints=critical,CA:FALSE",
            "-keyout", str(cases / "api.key"), "-out", str(cases / "api.pem"), capture_output=True)
        os.chown(cases / "api.key", 10001, 10001)
        (cases / "api.key").chmod(0o600)
        config = {"pebble": {"listenAddress": "0.0.0.0:14000", "managementListenAddress": "0.0.0.0:15000",
                  "certificate": "/cases/api.pem", "privateKey": "/cases/api.key", "httpPort": 10080, "tlsPort": 10443,
                  "externalAccountBindingRequired": False, "profiles": {"default": {"description": "renewal fixture", "validityPeriod": args.validity}}, "keyAlgorithm": "ecdsa"}}
        (cases / "pebble.json").write_text(json.dumps(config))
        run("docker", "network", "create", name, capture_output=True)
        network = True
        network_info = json.loads(run("docker", "network", "inspect", name, capture_output=True, text=True).stdout)[0]
        gateway = network_info["IPAM"]["Config"][0]["Gateway"]
        edge = (ROOT / "deploy/mail/edge.toml").read_text().replace("mail.example.test", "mail.test").replace("admin.example.test", "admin.test")
        edge = edge.replace('allow=["192.0.2.0/24", "2001:db8::/32"]', f'allow=["{gateway}/32"]')
        edge = edge.replace("drain_seconds = 30", "drain_seconds = 1").replace("drain_seconds = 0", "drain_seconds = 1")
        if args.stress:
            edge = edge.replace("max_connections_per_ip = 100", "max_connections_per_ip = 4096")
        if args.dns01:
            edge = edge.replace('server_names = ["mail.test"]', 'server_names = ["mail.test", "*.mail.test"]')
        (cases / "edge.toml").write_text(edge)
        agent = (ROOT / "deploy/mail/certd.toml").read_text().replace("mail.example.test", "mail.test").replace("admin.example.test", "admin.test")
        agent = agent.replace('directory_url = "https://acme-v02.api.letsencrypt.org/directory"', 'directory_url = "https://ca.test:14000/dir"\ndirectory_ca = "/cases/api.pem"')
        agent = agent.replace("renew_before_seconds = 2592000", f"renew_before_seconds = {max(120, args.validity // 3)}").replace("retry_seconds = 300", "retry_seconds = 60")
        (cases / "certd.toml").write_text(agent)
        if args.backend_image or args.dns01:
            import secrets
            token=cases/"reload-token";token.write_text(secrets.token_hex(32));os.chown(token,10001,10001);token.chmod(0o600)
        if args.backend_image:
            for target, protocol, address in (("smtp","smtp_starttls","mail-backend:1025"),("imap","imap_starttls","mail-backend:1143")):
                agent+=f'''\n[[certificates.delivery]]
id="{target}"
address="{address}"
protocol="{protocol}"
notify_url="https://backend.test:8777/reload"
bearer_file="/cases/reload-token"
[certificates.delivery.tls]
server_name="mail.test"
ca_certificate="/cases/issuer-root.pem"
[certificates.delivery.notify_tls]
server_name="backend.test"
ca_certificate="/cases/api.pem"
'''
            (cases/"certd.toml").write_text(agent)
        mounts = [f"{cases}:/cases:ro"]
        ca_command=["-config", "/cases/pebble.json"]
        if args.dns01:
            launch("dns",args.fixture_image,["python3","-u","/provider.py"],mounts+[f"{ROOT/'scripts/fixtures/dns_provider.py'}:/provider.py:ro"],aliases=["dns.test"])
            dns_ip=inspect(names["dns"])["NetworkSettings"]["Networks"][name]["IPAddress"]
            ca_command += ["-dnsserver",f"{dns_ip}:8053"]
            agent=agent.replace('domains = ["mail.test", "admin.test"]','domains = ["mail.test", "admin.test", "*.mail.test"]\nchallenge="dns01"')
            agent=agent.replace('[[certificates]]',f'''[dns01]
present_url="https://dns.test:8888/present"
cleanup_url="https://dns.test:8888/cleanup"
bearer_file="/cases/reload-token"
nameservers=["{dns_ip}:8053"]
propagation_timeout_seconds=30
[dns01.tls]
server_name="dns.test"
ca_certificate="/cases/api.pem"
[[certificates]]''',1)
            (cases/"certd.toml").write_text(agent)
        launch("ca", PEBBLE, ca_command, mounts, [15000], ["ca.test"], ["-e", "PEBBLE_VA_NOSLEEP=1", "-e", "PEBBLE_WFE_NONCEREJECT=0"])
        launch("backend", args.backend_image or args.fixture_image, [] if args.backend_image else ["python3", "-u", "/fixture.py"],
               [f"{ROOT / 'scripts/fixtures/mail_stack.py'}:/fixture.py:ro", f"{work / 'certificates'}:/certificates:ro", *mounts], aliases=["mail-backend", "admin-backend", "backend.test"])
        launch("certd", args.image, ["run", "--config", "/cases/certd.toml"],
               mounts + [f"{work / 'certd-state'}:/var/lib/prost-certd", f"{work / 'certificates'}:/certificates", f"{work / 'socket'}:/run/prost"],
               aliases=["certd"], extra=["--entrypoint", "/usr/local/bin/prost-certd"])
        for _ in range(30):
            check = subprocess.run(["docker", "exec", names["certd"], "prost-certd", "check"], capture_output=True, timeout=5)
            if check.returncode == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("certificate agent did not bootstrap")
        launch("edge", args.image, ["prost", "run", "--config", "/var/lib/prost/config.toml", "--seed", "/cases/edge.toml"],
               mounts + [f"{work / 'state'}:/var/lib/prost", f"{work / 'certificates'}:/certificates:ro", f"{work / 'socket'}:/run/prost"],
               [10025, 10587, 10143, 10465, 10993, 10443, 10080], ["mail.test", "admin.test"])
        asyncio.run(workload(args, out, cases, names, port, ctl, report, started))
        report["state"] = "passed"
    except BaseException as error:
        report.update(state="interrupted" if isinstance(error, KeyboardInterrupt) else "failed", error=repr(error), errors=report["errors"] + int(not isinstance(error, KeyboardInterrupt)))
        try:
            report["failure_status"] = ctl("status")
            report["failure_containers"] = {role: inspect(container)["State"] for role, container in names.items() if container in created}
            report["failure_resources"] = {}
            for role, container in names.items():
                if container not in created:continue
                monitor=Container(container)
                report["failure_resources"][role]={**monitor.sample(), "memory_events":(monitor.group/"memory.events").read_text(), "memory_peak_bytes":int((monitor.group/"memory.peak").read_text())}
        except (subprocess.SubprocessError, OSError, ValueError):
            pass
        raise
    finally:
        report["elapsed_seconds"] = time.monotonic() - started
        save(out / "summary.json", report)
        save(out / "progress.json", report)
        for container in reversed(created):
            result = subprocess.run(["docker", "logs", container], capture_output=True, text=True, timeout=30)
            (out / (container.rsplit("-", 1)[1] + ".log")).write_text(result.stdout + result.stderr)
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=30)
        if network:
            subprocess.run(["docker", "network", "rm", name], capture_output=True, timeout=30)
        # Only this process's securely created temporary directory is removed.
        shutil.rmtree(work)
        print(json.dumps(report, indent=2), flush=True)


async def workload(args, out, cases, names, port, ctl, report, started):
    ports = {p: port(names["edge"], p) for p in (10025, 10587, 10143, 10465, 10993, 10443, 10080)}
    api_port = port(names["ca"], 15000)
    api = ssl.create_default_context(cafile=str(cases / "api.pem"))
    for attempt in range(30):
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", api_port, ssl=api, server_hostname="ca.test")
            break
        except OSError:
            if attempt == 29:
                raise
            await asyncio.sleep(1)
    writer.write(b"GET /roots/0 HTTP/1.1\r\nHost: ca.test\r\nConnection: close\r\n\r\n")
    await writer.drain()
    code, root, _ = await response(reader)
    assert code == 200 and b"BEGIN CERTIFICATE" in root, (code, root)
    writer.close()
    await writer.wait_closed()
    (out / "issuer-root.pem").write_bytes(root)
    (cases / "issuer-root.pem").write_bytes(root)
    context = ssl.create_default_context(cafile=str(out / "issuer-root.pem"))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    # Wait for actual CA issuance and the durable TLS transaction, never accept
    # a self-signed bootstrap certificate as a successful ACME result.
    last = None
    for _ in range(120):
        try:
            r, w = await asyncio.open_connection("127.0.0.1", ports[10993], ssl=context, server_hostname="mail.test", ssl_handshake_timeout=3)
            assert (await r.readline()).startswith(b"* OK")
            first = w.get_extra_info("ssl_object").getpeercert(binary_form=True)
            w.close()
            await w.wait_closed()
            break
        except (OSError, AssertionError) as error:
            last = error
            await asyncio.sleep(1)
    else:
        raise AssertionError(f"CA certificate was not installed: {last!r}")
    report.update(state="running", started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), counts={"smtp": 0, "imap": 0, "http": 0, "h2": 0}, certificates=[hashlib.sha256(first).hexdigest()], checks=[])
    monitors = {role: Container(names[role]) for role in ("edge", "certd", "backend")}
    images = {role: json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["Image"] for role, name in names.items() if role!="dns" or args.dns01}
    report["images"] = images
    report["process_limits"] = {role: Path(f"/proc/{monitor.pid}/limits").read_text() for role, monitor in monitors.items()}
    deadline = time.monotonic() + args.duration

    async def connect(p, implicit=False):
        return await asyncio.open_connection("127.0.0.1", ports[p], ssl=context if implicit else None, server_hostname="mail.test" if implicit else None)

    async def command(reader, writer, value, prefix):
        writer.write(value)
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 5)
        assert line.startswith(prefix), (value, line)
        return line

    async def smtp(p, implicit):
        r, w = await connect(p, implicit)
        try:
            assert (await asyncio.wait_for(r.readline(), 5)).startswith(b"220")
            if not implicit:
                await command(r, w, b"EHLO fixture\r\n", b"250-")
                assert (await r.readline()).startswith(b"250-X-CLIENT-IP ")
                assert (await r.readline()).startswith(b"250 STARTTLS")
                await command(r, w, b"STARTTLS\r\n", b"220")
                await w.start_tls(context, server_hostname="mail.test", ssl_handshake_timeout=5)
            await command(r, w, b"EHLO secured\r\n", b"250-")
            identity = await r.readline()
            assert identity.strip() == f"250-X-CLIENT-IP {gateway}".encode(), identity
            assert (await r.readline()).startswith(b"250 ")
            await command(r, w, b"NOOP\r\n", b"250")
            await command(r, w, b"QUIT\r\n", b"221")
            report["counts"]["smtp"] += 1
        finally:
            w.close()
            try:
                await asyncio.wait_for(w.wait_closed(), 3)
            except (TimeoutError, OSError):
                w.transport.abort()

    # Linux bridge records the actual host-side peer address, not 127.0.0.1.
    network = json.loads(run("docker", "network", "inspect", names["edge"].removesuffix("-edge"), capture_output=True, text=True).stdout)[0]
    gateway = network["IPAM"]["Config"][0]["Gateway"]
    held = []
    for p, implicit in ((10143, False), (10993, True)):
        r, w = await connect(p, implicit)
        assert (await r.readline()).startswith(b"* OK")
        if not implicit:
            await command(r, w, b"a STARTTLS\r\n", b"a OK")
            await w.start_tls(context, server_hostname="mail.test", ssl_handshake_timeout=5)
        await command(r, w, b"idle IDLE\r\n", b"+ idling")
        held.append((r, w))
    smtp_reader, smtp_writer = await connect(10465, True)
    assert (await smtp_reader.readline()).startswith(b"220")

    async def web(host="admin.test", path="/", tls=True):
        r, w = await asyncio.open_connection("127.0.0.1", ports[10443 if tls else 10080], ssl=context if tls else None, server_hostname="admin.test" if tls else None)
        w.write(f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode())
        await w.drain()
        header = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), 5)
        body = await asyncio.wait_for(r.read(), 5)
        w.close()
        await w.wait_closed()
        return int(header.split()[1]), body, header.lower()

    assert (await web("wrong.test"))[0] == 421
    code, _, header = await web("mail.test", "/folder?a=1", False)
    assert code == 308 and b"location: https://mail.test/folder?a=1\r\n" in header, (code, header)
    assert (await web("mail.test", "/.well-known/acme-challenge/not-a-token", False))[0] == 404
    code, _, header = await web()
    assert code == 200 and b"strict-transport-security" not in header and b"content-security-policy" not in header
    # A second network peer is outside the deliberately exact /32 admin ACL.
    denial = "import socket,ssl; c=ssl.create_default_context(cafile='/cases/issuer-root.pem'); s=c.wrap_socket(socket.create_connection(('admin.test',10443)),server_hostname='admin.test'); s.sendall(b'GET / HTTP/1.1\\r\\nHost: admin.test\\r\\nConnection: close\\r\\n\\r\\n'); assert s.recv(1024).split()[1]==b'403'"
    await asyncio.to_thread(run, "docker", "run", "--rm", "--network", names["edge"].removesuffix("-edge"),
                            "--user", "10001:10001", "--read-only", "--cap-drop", "ALL",
                            "-v", f"{cases}:/cases:ro", args.fixture_image, "python3", "-c", denial, capture_output=True)
    # Strict SNI must remain listener-specific even though one SAN certificate
    # contains both mail and admin DNS names.
    for p, name in ((10993, "admin.test"), (10443, "mail.test"), (10993, "unknown.test"), (10993, None)):
        negative = ssl.create_default_context(cafile=str(out / "issuer-root.pem"))
        negative.check_hostname = False
        try:
            _, w = await asyncio.open_connection("127.0.0.1", ports[p], ssl=negative, server_hostname=name, ssl_handshake_timeout=3)
        except (ssl.SSLError, ConnectionError):
            pass
        else:
            w.close()
            await w.wait_closed()
            raise AssertionError(f"strict SNI accepted {p}/{name}")
    if args.backend_image:
        for _ in range(40):
            check=await asyncio.to_thread(subprocess.run,["docker","exec",names["certd"],"prost-certd","check","--certificates"],capture_output=True,timeout=10)
            if check.returncode==0:break
            await asyncio.sleep(1)
        else:raise AssertionError("backend certificates never acknowledged")
        report["checks"].append("backend fingerprint ACK, not notification response, controls certificate readiness")
    report["checks"] += ["verified DNS-01 wildcard issuance" if args.dns01 else "verified HTTP-01 issuance through edge", "strict listener-specific SNI", "HTTP Host 421", "HTTP CIDR 403 from second peer", "HTTP 308 query preservation", "unknown challenge 404", "application controls CSP/HSTS"]
    if args.dns01:
        r, w = await asyncio.open_connection("127.0.0.1", ports[10993], ssl=context, server_hostname="tenant.mail.test", ssl_handshake_timeout=5)
        assert (await r.readline()).startswith(b"* OK")
        w.close()
        await asyncio.wait_for(w.wait_closed(), 3)
        report["checks"].append("wildcard certificate and strict single-label tenant SNI verified")
    baseline = {role: monitor.sample()["cpu_usec"] for role, monitor in monitors.items()}
    for role, monitor in monitors.items():
        info = monitor.info
        assert info["Config"]["User"] == "10001:10001"
        assert info["HostConfig"]["ReadonlyRootfs"] and info["HostConfig"]["CapDrop"] == ["ALL"]
        assert "no-new-privileges" in info["HostConfig"]["SecurityOpt"]
    report["checks"].append("non-root, read-only rootfs, all capabilities dropped")
    rounds = 0
    restarted = False
    if args.backend_image:
        (cases/"reload-suppressed").touch()
    stress = None
    if args.stress:
        from mail_stress_load import exercise
        stress = asyncio.create_task(exercise(args, out, ports[10443], monitors, report, deadline))
    try:
        while time.monotonic() < deadline:
            if stress is not None and stress.done():
                await stress
            async with asyncio.timeout(30):
                async with asyncio.TaskGroup() as jobs:
                    for _ in range(args.clients):
                        for p in (10025, 10587, 10465):
                            jobs.create_task(smtp(p, p == 10465))
                await command(smtp_reader, smtp_writer, b"NOOP\r\n", b"250")
                # The second IMAPS session remains application-silent across
                # renewals. TCP keepalive does not count as relay activity.
                assert not held[1][0].at_eof(), "silent IMAP IDLE was disconnected"
                for r, w in held[:1]:
                    await command(r, w, b"DONE\r\n", b"idle OK")
                    await command(r, w, b"n NOOP\r\n", b"n OK")
                    await command(r, w, b"idle IDLE\r\n", b"+ idling")
                    report["counts"]["imap"] += 1
                code, body, _ = await web()
                assert code == 200 and json.loads(body)["source"] == gateway, (code, body)
                report["counts"]["http"] += 1
                r, w = await connect(10993, True)
                digest = hashlib.sha256(w.get_extra_info("ssl_object").getpeercert(binary_form=True)).hexdigest()
                if digest not in report["certificates"]:
                    report["certificates"].append(digest)
                assert (await r.readline()).startswith(b"* OK")
                w.close()
                await w.wait_closed()
            status = await asyncio.to_thread(ctl, "status")
            assert status["worker_generations"]["forced_total"] == 0, status
            assert status["live_generations"] <= 3, status
            if len(report["certificates"]) >= 2 and not restarted:
                if args.backend_image:
                    for _ in range(2):
                        check=await asyncio.to_thread(subprocess.run,["docker","exec",names["certd"],"prost-certd","check","--certificates"],capture_output=True,timeout=10)
                        assert check.returncode != 0, "notification HTTP 200 incorrectly acknowledged stale backend cert"
                        await asyncio.sleep(6)
                    report["checks"].append("notification HTTP 200 with stale backend fingerprint does not acknowledge delivery")
                report["agent_restarting"] = True
                await asyncio.to_thread(run, "docker", "restart", names["certd"], capture_output=True)
                if args.backend_image:
                    (cases/"reload-suppressed").unlink()
                    for _ in range(40):
                        check=await asyncio.to_thread(subprocess.run,["docker","exec",names["certd"],"prost-certd","check","--certificates"],capture_output=True,timeout=10)
                        if check.returncode==0:break
                        await asyncio.sleep(1)
                    else:raise AssertionError("persisted delivery outbox did not recover after restart")
                    report["checks"].append("pending certificate delivery survives agent restart and converges to verified ACK")
                monitors["certd"] = Container(names["certd"])
                baseline["certd"] = monitors["certd"].sample()["cpu_usec"]
                report["agent_restarting"] = False
                restarted = True
                report["checks"].append("agent restart restores private account/certificate state")
            if rounds % 6 == 0:
                code, body, _ = await web(path="/stats")
                stats = json.loads(body)
                assert code == 200 and stats["local"] > 0 and stats["invalid"] == 0 and stats["starttls"] > 0, stats
                report["backend"] = stats
                if args.backend_image and len(report["certificates"]) >= 2:
                    assert stats["reloads"] > 0, "backend cached certificate was never reloaded"
                h2 = await asyncio.to_thread(run, "h2load", "-n", "20", "-c", "2", "--connect-to", f"127.0.0.1:{ports[10443]}", f"https://admin.test:{ports[10443]}/", capture_output=True, text=True)
                assert "20 succeeded, 0 failed" in h2.stdout, h2.stdout
                report["counts"]["h2"] += 20
            points = {}
            for role, monitor in monitors.items():
                point = monitor.sample()
                point["rss_bytes"] = int(next(line.split()[1] for line in Path(f"/proc/{monitor.pid}/status").read_text().splitlines() if line.startswith("VmRSS:"))) * 1024
                point["cpu_elapsed_usec"] = point["cpu_usec"] - baseline[role]
                points[role] = point
            report.update(elapsed_seconds=time.monotonic() - started, status=status, resources=points)
            with (out / "samples.jsonl").open("a") as samples:
                samples.write(json.dumps({"elapsed": report["elapsed_seconds"], "resources": points, "generation": status.get("generation"), "certificates": len(report["certificates"])}) + "\n")
            save(out / "progress.json", report)
            rounds += 1
            await asyncio.sleep(args.interval)
        assert len(report["certificates"]) >= 2, "no verified renewal observed"
        if stress is not None:
            await asyncio.wait_for(asyncio.shield(stress), 45)
        if args.duration>=args.idle_silence_seconds:
            report["checks"].append(f"silent IMAPS IDLE held for at least {args.idle_silence_seconds}s")
        report["checks"] += ["CA renewal verified by fresh TLS connections", "held SMTP and STARTTLS/IMAPS IDLE survived renewal", "strict PROXY v2 traffic and zero-payload LOCAL health", "no forced worker drain", "SMTP source IP preserved", "HTTP/2 ALPN traffic"]
    finally:
        if stress is not None:
            if not stress.done():
                stress.cancel()
            await asyncio.gather(stress, return_exceptions=True)
        smtp_writer.close()
        try:
            await asyncio.wait_for(smtp_writer.wait_closed(), 3)
        except (TimeoutError, OSError):
            smtp_writer.transport.abort()
        for r, w in held:
            try:
                await command(r, w, b"DONE\r\n", b"idle OK")
            finally:
                w.close()
                try:
                    await asyncio.wait_for(w.wait_closed(), 3)
                except (TimeoutError, OSError):
                    w.transport.abort()


if __name__ == "__main__":
    main()
