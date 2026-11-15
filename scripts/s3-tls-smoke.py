"""Docker/WSL release test: native S3/SigV4, presigned HTTP, file URI, rolling TLS.

Uses a private UUID RustFS fixture and disposable credentials. No AWS account.
"""
import argparse
import json
from pathlib import Path
import socket
import ssl
import subprocess
import time
import urllib.request
import uuid
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parent.parent

def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--s3-image", default="rustfs/rustfs:latest")
    parser.add_argument("--aws-image", default="amazon/aws-cli:latest")
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/s3-tls-smoke")
    args = parser.parse_args(); out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=False)
    name = "prost-s3-tls-" + uuid.uuid4().hex[:10]; s3 = name+"-s3"; backend = name+"-backend"
    key_id = "fixture" + uuid.uuid4().hex[:16]; secret = uuid.uuid4().hex + uuid.uuid4().hex
    created, clients, commands, checks = [], [], [], []
    for number in [1, 2]:
        directory = out/f"certs/{number}"; directory.mkdir(parents=True)
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "2", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost", "-addext", "basicConstraints=critical,CA:FALSE", "-keyout", str(directory/"key.pem"), "-out", str(directory/"cert.pem"), capture_output=True)
    (out/"aws-config").write_text("[default]\nregion = us-east-1\ns3 =\n    addressing_style = path\n")
    def aws(*command):
        response = run("docker", "run", "--rm", "--network", name, "-v", f"{out}:/cases:ro",
            "-e", f"AWS_ACCESS_KEY_ID={key_id}", "-e", f"AWS_SECRET_ACCESS_KEY={secret}", "-e", "AWS_CONFIG_FILE=/cases/aws-config", "-e", "AWS_PAGER=",
            "-e", "AWS_MAX_ATTEMPTS=1", "-e", "AWS_REQUEST_CHECKSUM_CALCULATION=when_required", "-e", "AWS_RESPONSE_CHECKSUM_VALIDATION=when_required",
            args.aws_image, "--endpoint-url", "http://s3:9000", *command, capture_output=True, text=True)
        return response.stdout
    def ctl(*command, fail=False):
        response = subprocess.run(["docker", "exec", name, "prostctl", *command], capture_output=True, text=True, timeout=60)
        commands.append({"command": list(command), "code": response.returncode, "stdout": response.stdout, "stderr": response.stderr})
        if fail:
            assert response.returncode != 0; return
        response.check_returncode(); return json.loads(response.stdout)["result"]
    def inspect(container): return json.loads(run("docker", "inspect", container, capture_output=True, text=True).stdout)[0]
    def port(info): return int(info["NetworkSettings"]["Ports"]["9000/tcp"][0]["HostPort"])
    def ready():
        for _ in range(100):
            try:
                status = ctl("status")
                if status["upstreams"]["echo"][0]["healthy"]:
                    return status
            except (subprocess.CalledProcessError, json.JSONDecodeError): time.sleep(.1)
            else: time.sleep(.1)
        raise RuntimeError("proxy did not become ready")
    def connect(number):
        context = ssl.create_default_context(cafile=str(out/f"certs/{number}/cert.pem"))
        plain = socket.create_connection(("127.0.0.1", port(inspect(name))), 5)
        try: client = context.wrap_socket(plain, server_hostname="localhost")
        except BaseException: plain.close(); raise
        clients.append(client); return client
    def echo(client, data):
        client.sendall(data); actual = b""
        while len(actual) < len(data):
            chunk = client.recv(len(data)-len(actual)); assert chunk; actual += chunk
        assert actual == data
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=5
reload_mode="rolling"
[runtime.s3]
region="us-east-1"
endpoint="http://s3:9000"
force_path_style=true
[upstreams.echo]
health_interval_ms=100
[[upstreams.echo.endpoints]]
id="one"
address="backend:9000"
[[listeners]]
id="tls"
address="0.0.0.0:9000"
protocol="tcp"
upstream="echo"
[listeners.tls]
certificate="s3://prost-certs/tls/fullchain.pem"
private_key="file:///cases/certs/1/key.pem"
'''
    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(s3)
        run("docker", "run", "-d", "--name", s3, "--network", name, "--network-alias", "s3", "--cpus", "2", "--memory", "1g",
            "-e", f"RUSTFS_ACCESS_KEY={key_id}", "-e", f"RUSTFS_SECRET_KEY={secret}", "-e", "RUSTFS_VOLUMES=/data", "-e", "RUSTFS_ADDRESS=0.0.0.0:9000", "-e", "RUSTFS_BOOTSTRAP_ENDPOINT=http://127.0.0.1:9000",
            "-p", "127.0.0.1::9000", args.s3_image, "/data", stdout=subprocess.DEVNULL)
        s3_info = inspect(s3)
        for _ in range(120):
            try:
                # CLI-only test fixture URL, controlled by the test/operator; no remote user input or production TLS artifact fetching.
                # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                with urllib.request.urlopen(f"http://127.0.0.1:{port(s3_info)}/health/ready", timeout=1) as response:
                    if response.status == 200: break
            except OSError: pass
            time.sleep(.25)
        else: raise RuntimeError("S3 fixture did not become ready")
        aws("s3api", "create-bucket", "--bucket", "prost-certs")
        aws("s3api", "put-bucket-versioning", "--bucket", "prost-certs", "--versioning-configuration", "Status=Enabled")
        version = json.loads(aws("s3api", "put-object", "--bucket", "prost-certs", "--key", "tls/fullchain.pem", "--body", "/cases/certs/1/cert.pem"))["VersionId"]
        aws("s3api", "put-object", "--bucket", "prost-certs", "--key", "tls/priv key.pem", "--body", "/cases/certs/1/key.pem")
        # Prove the fixture enforces authentication; a successful signed load
        # must not be explainable by an accidentally public object.
        try:
            # CLI-only test fixture URL, controlled by the test/operator; no remote user input or production TLS artifact fetching.
            # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
            with urllib.request.urlopen(f"http://127.0.0.1:{port(s3_info)}/prost-certs/tls/fullchain.pem", timeout=3):
                raise AssertionError("S3 fixture allowed unsigned object GET")
        except HTTPError as error:
            assert error.code == 403, error.code
            error.close()
        presigned = aws("s3", "presign", "s3://prost-certs/tls/fullchain.pem", "--expires-in", "3600").strip()
        seed += '\n[[listeners.tls.certificates]]\nserver_names=["localhost"]\ncertificate=' + json.dumps(presigned) + '\nprivate_key="s3://prost-certs/tls/priv%20key.pem"\n'
        (out/"seed.toml").write_text(seed)
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--memory", "64m", "-v", f"{ROOT/'examples/backend.py'}:/backend.py:ro", "python:3.13-alpine", "python", "/backend.py", stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2",
            "-e", f"AWS_ACCESS_KEY_ID={key_id}", "-e", f"AWS_SECRET_ACCESS_KEY={secret}", "-e", "AWS_EC2_METADATA_DISABLED=true",
            "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-p", "127.0.0.1::9000", args.image, stdout=subprocess.DEVNULL)
        assert ready()["worker_generation"] == 0
        old = connect(1); echo(old, b"S3 and presigned HTTP plus file URI")
        checks.append("private S3 SigV4 GET, percent-encoded key, presigned GET query and file URI all load usable TLS material")
        aws("s3api", "put-object", "--bucket", "prost-certs", "--key", "tls/fullchain.pem", "--body", "/cases/certs/2/cert.pem")
        latest = seed.split('\n[[listeners.tls.certificates]]')[0]
        pinned = latest.replace('s3://prost-certs/tls/fullchain.pem', 's3://prost-certs/tls/fullchain.pem?versionId='+version)
        (out/"pinned.toml").write_text(pinned)
        ctl("reload", "--file", "/cases/pinned.toml", "--if-generation", "0", "--wait", "--wait-timeout", "1", fail=True)
        assert ctl("status")["generation"] == 1
        pinned_client = connect(1); echo(pinned_client, b"pinned object version"); pinned_client.close()
        echo(old, b"old TLS stream survives rolling"); old.close()
        latest = latest.replace("file:///cases/certs/1/key.pem", "file:///cases/certs/2/key.pem")
        (out/"latest.toml").write_text(latest)
        applied = ctl("reload", "--file", "/cases/latest.toml", "--if-generation", "1", "--wait", "--wait-timeout", "10")
        assert applied["worker_drain"]["drained"] is True
        assert ctl("workers", "wait", "--generation", "2", "--timeout", "10")["drained"] is True
        new = connect(2); echo(new, b"latest S3 certificate"); new.close()
        assert ctl("status")["worker_generation"] == 2
        checks.append("versionId pins old object; explicit reload fetches latest object into a new worker generation; old TLS session drains")
        checks.append("CLI reload --wait timeout preserves commit and old TLS stream; subsequent --wait and workers wait complete without polling")
        (out/"bad.toml").write_text(latest.replace("tls/fullchain.pem", "missing.pem"))
        ctl("validate", "--file", "/cases/bad.toml", fail=True)
        ctl("apply", "--file", "/cases/bad.toml", "--if-generation", "2", fail=True)
        assert ctl("status")["generation"] == 2
        good = connect(2); echo(good, b"failed S3 load keeps current workers"); good.close()
        run("docker", "stop", "-t", "1", s3, stdout=subprocess.DEVNULL)
        run("docker", "restart", name, stdout=subprocess.DEVNULL); assert ready()["generation"] == 2
        good = connect(2); echo(good, b"offline artifact restart"); good.close()
        checks.append("missing object rejects validate/apply without commit; restart works after S3 becomes unavailable")
        summary={"state":"passed", "image_id":inspect(name)["Image"], "s3_image_id":s3_info["Image"], "checks":checks, "generation":2, "method":"local functional checks; no AWS account or capacity benchmark"}
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n"); print(json.dumps(summary,indent=2),flush=True)
    finally:
        # Reports contain command results, never candidate contents or AWS credentials.
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for client in clients: client.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log: subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)

if __name__=="__main__": main()
