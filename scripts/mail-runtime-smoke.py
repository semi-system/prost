"""Native image: low ports, read-only rootfs, zero capabilities, TLS refresh and stdout.

Uses only UUID test containers/network and disposable certificates. Does not
connect to a real mail service or disturb a VM soak campaign.
"""
import argparse
import json
from pathlib import Path
import socket
import ssl
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:mail-next")
    parser.add_argument("--fixture-image", default="python:3.13-alpine")
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/mail-runtime-smoke")
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    name = "prost-mail-runtime-"+uuid.uuid4().hex[:10]
    backend = name+"-backend"
    created = []
    clients = []
    report = {"state":"starting"}

    def ctl(*command, expected=True):
        result = subprocess.run(["docker", "exec", name, "prostctl", *command], capture_output=True, text=True, timeout=30)
        if not expected:
            assert result.returncode != 0, result.stdout
            return
        result.check_returncode()
        return json.loads(result.stdout)["result"]

    def check(io):
        body = b"EHLO fixture\r\nSTARTTLS\r\nopaque\x00bytes"
        io.sendall(body)
        response = bytearray()
        while len(response) < len(body):
            chunk = io.recv(len(body)-len(response))
            assert chunk, "session closed across TLS refresh"
            response.extend(chunk)
        assert response == body

    def inspect():
        return json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]

    try:
        for label in ["a", "b"]:
            run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
                "-days", "2", "-subj", "/CN=mail.test", "-addext", "subjectAltName=DNS:mail.test",
                "-keyout", str(out/f"{label}.key"), "-out", str(out/f"{label}.pem"), capture_output=True)
            # Disposable test keys shared with the UID 10001 fixture only.
            (out/f"{label}.key").chmod(0o644)

        def tls(label):
            return f'''certificate="/cases/{label}.pem"
private_key="/cases/{label}.key"
reject_unknown_sni=true
[[certificates]]
server_names=["mail.test"]
certificate="/cases/{label}.pem"
private_key="/cases/{label}.key"
[policy]
min_version="tls12"
session_resumption=false
'''
        (out/"tls-b.toml").write_text(tls("b"))
        (out/"bad.toml").write_text(tls("b").replace('private_key="/cases/b.key"', 'private_key="/cases/a.key"'))
        config = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
reload_mode="rolling"
io_workers=1
drain_seconds=1
[runtime.access_log]
sink="stdout"
flush_interval_ms=100
queue_capacity=128
[[upstreams.mail.endpoints]]
id="one"
address="backend:9000"
[[listeners]]
id="smtp"
address="0.0.0.0:25"
protocol="tcp"
upstream="mail"
[[listeners]]
id="mail"
address="0.0.0.0:993"
protocol="tcp"
upstream="mail"
[listeners.tls]
'''+tls("a").replace("[[certificates]]", "[[listeners.tls.certificates]]").replace("[policy]", "[listeners.tls.policy]")
        (out/"seed.toml").write_text(config)
        run("docker", "network", "create", name, capture_output=True)
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend",
            "--memory", "64m", "--cpus", "0.5", "-v", f"{ROOT/'examples/backend.py'}:/backend.py:ro",
            args.fixture_image, "python", "/backend.py", capture_output=True)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name,
            "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--user", "10001:10001",
            "--sysctl", "net.ipv4.ip_unprivileged_port_start=0",
            "--tmpfs", "/run/prost:rw,noexec,nosuid,size=4m,mode=0700,uid=10001,gid=10001",
            "--memory", "256m", "--cpus", "2", "--stop-timeout", "5",
            "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro",
            "-p", "127.0.0.1::25", "-p", "127.0.0.1::993", args.image, capture_output=True)
        for _ in range(100):
            try:
                status = ctl("status")
                if status["upstreams"]["mail"][0]["healthy"]:
                    break
            except (subprocess.CalledProcessError, json.JSONDecodeError):
                pass
            time.sleep(.1)
        else:
            raise RuntimeError("native proxy did not become ready")
        info = inspect()
        ports = {port:int(info["NetworkSettings"]["Ports"][f"{port}/tcp"][0]["HostPort"]) for port in [25,993]}

        def secure(label):
            context = ssl.create_default_context(cafile=str(out/f"{label}.pem"))
            io = context.wrap_socket(socket.create_connection(("127.0.0.1", ports[993]), timeout=5), server_hostname="mail.test")
            clients.append(io)
            return io

        plain = socket.create_connection(("127.0.0.1", ports[25]), timeout=5)
        clients.append(plain)
        old = secure("a")
        check(plain)
        check(old)
        update = ctl("tls", "reload", "mail", "--file", "/cases/tls-b.toml", "--if-generation", "0")
        assert update["generation"] == 1 and update["worker_drain_started"] is False
        fresh = secure("b")
        check(fresh)
        time.sleep(1.2)
        check(old)
        check(plain)
        ctl("tls", "reload", "mail", "--file", "/cases/bad.toml", "--if-generation", "1", expected=False)
        status = ctl("status")
        assert status["generation"] == 1 and status["worker_generations"]["forced_total"] == 0
        for io in clients:
            io.close()
        clients.clear()
        for _ in range(50):
            status = ctl("status")
            if status["connections"] == 0:
                break
            time.sleep(.05)
        else:
            raise AssertionError("closed sessions did not finish")
        ctl("access-log", "flush", "--if-generation", "1")
        ctl("access-log", "reopen", "--if-generation", "1")
        raw = run("docker", "logs", name, capture_output=True, text=True).stdout
        (out/"stdout.log").write_text(raw)
        records = [json.loads(line) for line in raw.splitlines() if line.startswith('{')]
        access = [record for record in records if record.get("protocol") == "tcp"]
        assert len(access) >= 3 and {r["listener"] for r in access} == {"mail","smtp"}, access
        probe = """import json
from pathlib import Path
status=dict(line.split(':',1) for line in Path('/proc/1/status').read_text().splitlines() if ':' in line)
root=next(line.split()[5] for line in Path('/proc/1/mountinfo').read_text().splitlines() if line.split()[4]=='/')
print(json.dumps({'uid':status['Uid'].split()[0],'effective':int(status['CapEff'],16),'bounding':int(status['CapBnd'],16),'no_new_privileges':status['NoNewPrivs'].strip(),'root_mount':root}))
"""
        actual = json.loads(run("docker", "run", "--rm", "--pid", "container:"+name, "--network", "none", "--user", "10001:10001",
            "--cap-drop", "ALL", "--memory", "64m", args.fixture_image, "python", "-c", probe, capture_output=True, text=True).stdout)
        assert actual["uid"] == "10001" and actual["effective"] == actual["bounding"] == 0
        assert actual["no_new_privileges"] == "1" and "ro" in actual["root_mount"].split(',')
        assert info["HostConfig"]["ReadonlyRootfs"] and info["HostConfig"]["CapDrop"] == ["ALL"]
        report.update(state="passed", image_id=info["Image"], kernel=actual, tls_update=update, access_records=len(access), ports=ports)
    except BaseException as error:
        report.update(state="failed", error=repr(error))
        raise
    finally:
        for io in clients:
            io.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log:
                subprocess.run(["docker","logs",container], stdout=log, stderr=subprocess.STDOUT, timeout=30)
            subprocess.run(["docker","rm","-f","-v",container], capture_output=True, timeout=30)
        subprocess.run(["docker","network","rm",name], capture_output=True, timeout=30)
        (out/"summary.json").write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
