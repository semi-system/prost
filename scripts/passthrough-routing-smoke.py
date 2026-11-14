"""Local release-image SNI routing checks with two independent TLS backends.

Uses verified disposable certificates, real CLI apply/restart and TLS keepalive.
No performance measurement; creates/cleans only its UUID containers/network.
"""
import argparse
import hashlib
from http.client import HTTPConnection
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


def certificates(out):
    run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-days", "2", "-subj", "/CN=Prost routing test CA", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", str(out/"ca.key"), "-out", str(out/"ca.pem"), capture_output=True)
    names = ["api.example.test", "x.example.test", "example.test", "deep.x.example.test", "fallback.other.test", "unknown.test"]
    fingerprints = {}
    for serial, label in enumerate(("blue", "green"), 1):
        directory = out/label
        directory.mkdir()
        extensions = directory/"extensions.cnf"
        extensions.write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=" + ",".join("DNS:"+name for name in names) + "\n")
        run("openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes", "-subj", "/CN=api.example.test", "-keyout", str(directory/"key.pem"), "-out", str(directory/"request.csr"), capture_output=True)
        run("openssl", "x509", "-req", "-in", str(directory/"request.csr"), "-CA", str(out/"ca.pem"), "-CAkey", str(out/"ca.key"), "-set_serial", str(serial), "-days", "2", "-extfile", str(extensions), "-out", str(directory/"cert.pem"), capture_output=True)
        # Disposable fixtures must be readable by the backend image's UID 10001.
        (directory/"key.pem").chmod(0o644)
        fingerprints[label] = hashlib.sha256(ssl.PEM_cert_to_DER_cert((directory/"cert.pem").read_text())).hexdigest()
    return fingerprints


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--backend-image", default="prost:bundle-backend")
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/passthrough-routing-smoke")
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    fingerprints = certificates(out)
    name = "prost-sni-route-"+uuid.uuid4().hex[:10]
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=1
[upstreams.blue]
health_interval_ms=60000
idle_timeout_ms=60000
[[upstreams.blue.endpoints]]
id="one"
address="blue:8443"
[upstreams.green]
health_interval_ms=60000
idle_timeout_ms=60000
[[upstreams.green.endpoints]]
id="one"
address="green:8443"
[[listeners]]
id="edge"
address="0.0.0.0:9443"
protocol="tcp"
upstream="blue"
tls_passthrough=true
[listeners.tls_passthrough_settings]
client_hello_timeout_ms=1000
reject_unknown_sni=true
server_names=["fallback.other.test"]
[[listeners.tls_passthrough_settings.routes]]
server_names=["*.example.test"]
upstream="green"
[[listeners.tls_passthrough_settings.routes]]
server_names=["API.EXAMPLE.TEST."]
upstream="blue"
'''
    (out/"seed.toml").write_text(seed)
    created, clients, commands, checks = [], [], [], []
    port = None

    def ctl(*command, failure=None):
        result = subprocess.run(["docker", "exec", name, "prostctl", *command], capture_output=True, text=True, timeout=30)
        commands.append({"command":list(command),"exit_code":result.returncode,"stdout":result.stdout,"stderr":result.stderr})
        if failure is not None:
            assert result.returncode != 0 and failure in result.stderr+result.stdout, commands[-1]
            return None
        result.check_returncode()
        return json.loads(result.stdout)["result"]

    def ready():
        for _ in range(100):
            try:
                return ctl("status")
            except subprocess.CalledProcessError:
                time.sleep(.1)
        raise RuntimeError("proxy did not become ready")

    def connect(host, label):
        context = ssl.create_default_context(cafile=str(out/"ca.pem"))
        context.check_hostname = host is not None
        context.set_alpn_protocols(["http/1.1"])
        raw = socket.create_connection(("127.0.0.1",port), timeout=5)
        try:
            tls = context.wrap_socket(raw, server_hostname=host)
        except BaseException:
            raw.close()
            raise
        fingerprint = hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest()
        assert fingerprint == fingerprints[label], (host,label,fingerprint)
        client = HTTPConnection(host or "no-sni", timeout=5)
        client.sock = tls
        clients.append(client)
        return client

    def get(client, label):
        client.request("GET", "/bytes/34")
        response = client.getresponse()
        body = response.read()
        assert response.status == 200 and body == (b"0123456789abcdef"*3)[:34]
        assert response.getheader("x-backend") == label

    def rejected(host):
        try:
            connect(host,"blue")
        except ssl.SSLCertVerificationError:
            raise AssertionError("certificate failure is not evidence of SNI rejection")
        except (ssl.SSLError,ConnectionError):
            return
        raise AssertionError(f"unexpectedly accepted SNI: {host}")

    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        for label in ("blue","green"):
            backend = name+"-"+label
            created.append(backend)
            run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", label, "--cpus", "1", "--memory", "128m", "-e", "GOMAXPROCS=1", "-e", f"BACKEND_LABEL={label}", "-v", f"{out/label}:/certs:ro", args.backend_image, stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-p", "127.0.0.1::9443", args.image, stdout=subprocess.DEVNULL)
        ready()
        info = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]
        port = int(info["NetworkSettings"]["Ports"]["9443/tcp"][0]["HostPort"])
        old = connect("api.example.test","blue")
        get(old,"blue")
        get(connect("x.example.test","green"),"green")
        get(connect("fallback.other.test","blue"),"blue")
        for host in ("unknown.test","example.test","deep.x.example.test",None):
            rejected(host)
        checks.append("exact/wildcard priority, allow-only fallback, strict unknown/missing/apex/deep rejection")
        with socket.create_connection(("127.0.0.1",port),timeout=3) as stalled:
            stalled.sendall(b"\x16\x03\x03\x00\x10\x01")
            try:
                assert stalled.recv(1) == b""
            except ConnectionResetError:
                pass
        checks.append("partial ClientHello deadline")
        rotated = seed.replace('server_names=["API.EXAMPLE.TEST."]\nupstream="blue"','server_names=["API.EXAMPLE.TEST."]\nupstream="green"')
        (out/"rotate.toml").write_text(rotated)
        ctl("apply", "--file", "/cases/rotate.toml", "--if-generation", "0")
        get(old,"blue")
        get(connect("api.example.test","green"),"green")
        checks.append("hot SNI map update pins existing TLS sessions to old backend")
        (out/"invalid.toml").write_text(rotated.replace('upstream="green"','upstream="missing"'))
        ctl("apply", "--file", "/cases/invalid.toml", "--if-generation", "1", failure="unknown passthrough route upstream")
        assert ctl("status")["generation"] == 1
        (out/"permissive.toml").write_text(rotated.replace("reject_unknown_sni=true","reject_unknown_sni=false"))
        ctl("apply", "--file", "/cases/permissive.toml", "--if-generation", "1")
        for host in ("unknown.test","example.test","deep.x.example.test",None):
            get(connect(host,"blue"),"blue")
        ctl("upstream", "disable", "green/one", "--if-generation", "2")
        rejected("x.example.test")
        get(connect("unknown.test","blue"),"blue")
        ctl("upstream", "enable", "green/one", "--if-generation", "3")
        get(connect("x.example.test","green"),"green")
        checks.append("invalid config rollback, permissive fallback and unavailable route isolation")
        for client in clients:
            client.close()
        run("docker", "restart", name, stdout=subprocess.DEVNULL)
        assert ready()["generation"] == 4
        restarted = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]
        port = int(restarted["NetworkSettings"]["Ports"]["9443/tcp"][0]["HostPort"])
        get(connect("api.example.test","green"),"green")
        checks.append("restart restores committed map and endpoint administration")
        summary = {"state":"passed","image_id":info["Image"],"checks":checks,"backend_certificate_sha256":fingerprints,"final_generation":4,"method":"functional checks with verified end-to-end TLS; no benchmark"}
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        print(json.dumps(summary,indent=2),flush=True)
    finally:
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for client in clients:
            client.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log:
                subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)


if __name__ == "__main__":
    main()
