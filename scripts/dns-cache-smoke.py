"""Local Docker/WSL release checks for system DNS refresh and conditional cache.

Only UUID containers/networks are modified. /etc/hosts inside the test proxy
provides deterministic getaddrinfo changes without depending on public DNS.
"""
import argparse
from http.client import HTTPConnection
import json
from pathlib import Path
import socket
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent

def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--fixture-image", default="python:3.13-alpine")
    parser.add_argument("--cert-dir", type=Path, help="Optional fixture certificates; otherwise generate a local CA and leaf")
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/dns-cache-smoke")
    args = parser.parse_args()
    out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=False)
    name = "prost-dns-cache-" + uuid.uuid4().hex[:10]
    created, clients, commands, checks = [], [], [], []
    cert_dir = args.cert_dir.resolve() if args.cert_dir else out/"certs"
    if args.cert_dir is None:
        cert_dir.mkdir()
        # Keys remain in this run's output directory and are only mounted into
        # its independent fixture. Proxy TLS config references the public CA.
        options = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes"]
        run("openssl", "req", "-x509", *options, "-days", "2", "-subj", "/CN=Prost smoke CA",
            "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", str(cert_dir/"ca.key"), "-out", str(cert_dir/"ca.pem"), capture_output=True)
        run("openssl", "req", "-new", *options, "-subj", "/CN=localhost", "-keyout", str(cert_dir/"key.pem"), "-out", str(cert_dir/"server.csr"), capture_output=True)
        (cert_dir/"server.ext").write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost\n")
        run("openssl", "x509", "-req", "-in", str(cert_dir/"server.csr"), "-CA", str(cert_dir/"ca.pem"), "-CAkey", str(cert_dir/"ca.key"),
            "-CAcreateserial", "-days", "2", "-extfile", str(cert_dir/"server.ext"), "-out", str(cert_dir/"cert.pem"), capture_output=True)
    def write(label, **values):
        target = out/f"state-{label}.json"
        current = json.loads(target.read_text()) if target.exists() else {}
        current.update(values)
        temporary = target.with_suffix(".tmp"); temporary.write_text(json.dumps(current)); temporary.replace(target)
    for label in ["a", "b"]: write(label, status=200, version=1, release=False)
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=1
[upstreams.web]
health_interval_ms=60000
[upstreams.web.dns]
ttl_ms=250
negative_ttl_ms=100
timeout_ms=300
[[upstreams.web.endpoints]]
id="one"
address="moving.test:8080"
[upstreams.web.http.cache]
revalidate=true
stale_if_error_seconds=3
max_ttl_seconds=60
[upstreams.secure]
health_interval_ms=60000
[upstreams.secure.dns]
ttl_ms=250
[upstreams.secure.tls]
server_name="localhost"
ca_certificate="/certs/cert.pem"
[[upstreams.secure.endpoints]]
id="one"
address="moving.test:8443"
[upstreams.tcp]
health_interval_ms=60000
[upstreams.tcp.dns]
ttl_ms=250
[[upstreams.tcp.endpoints]]
id="one"
address="moving.test:9090"
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
[[listeners]]
id="secure"
address="0.0.0.0:8081"
protocol="http"
upstream="secure"
[[listeners]]
id="tcp"
address="0.0.0.0:9090"
protocol="tcp"
upstream="tcp"
'''
    if (cert_dir/"ca.pem").exists(): seed = seed.replace('/certs/cert.pem', '/certs/ca.pem')
    (out/"seed.toml").write_text(seed)
    def ctl(*command):
        response = run("docker", "exec", name, "prostctl", *command, capture_output=True, text=True)
        commands.append({"command": list(command), "response": json.loads(response.stdout)})
        return commands[-1]["response"]["result"]
    def inspect(container):
        return json.loads(run("docker", "inspect", container, capture_output=True, text=True).stdout)[0]
    def port(info, number): return int(info["NetworkSettings"]["Ports"][f"{number}/tcp"][0]["HostPort"])
    def connect(number):
        client = HTTPConnection("127.0.0.1", number, timeout=5); clients.append(client); return client
    def get(client, path, expected, status=200):
        client.request("GET", path, headers={"host": "public.test"})
        response = client.getresponse(); body = response.read()
        assert response.status == status and body == expected, (response.status, body)
        return dict(response.getheaders())
    def map_host(ip):
        # Explicit arguments, no shell interpolation of user paths or config.
        run("docker", "exec", "-u", "0", name, "sh", "-c", 'printf "%s\\n" "$1" > /etc/hosts', "sh", f"127.0.0.1 localhost\n{ip} moving.test")
        time.sleep(.35)
    def calls(origin):
        origin.request("GET", "/stats"); response=origin.getresponse(); assert response.status == 200
        return json.loads(response.read())["calls"]
    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        for label in ["a", "b"]:
            backend = name+"-"+label; created.append(backend)
            run("docker", "run", "-d", "--name", backend, "--network", name,
                "--cpus", "1", "--memory", "128m", "-e", f"BACKEND_LABEL={label}",
                "-v", f"{ROOT/'scripts/fixtures/dns-cache-backend.py'}:/fixture.py:ro",
                "-v", f"{out}:/cases:ro", "-v", f"{cert_dir}:/certs:ro",
                "-p", "127.0.0.1::8080", args.fixture_image, "python", "/fixture.py", stdout=subprocess.DEVNULL)
        a, b = inspect(name+"-a"), inspect(name+"-b")
        ip_a=a["NetworkSettings"]["Networks"][name]["IPAddress"]; ip_b=b["NetworkSettings"]["Networks"][name]["IPAddress"]
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--add-host", f"moving.test:{ip_a}",
            "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2",
            "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro",
            "-v", f"{cert_dir}:/certs:ro", "-p", "127.0.0.1::8080", "-p", "127.0.0.1::8081", "-p", "127.0.0.1::9090", args.image, stdout=subprocess.DEVNULL)
        for _ in range(100):
            try: ctl("status"); break
            except (subprocess.CalledProcessError, json.JSONDecodeError): time.sleep(.1)
        else: raise RuntimeError("daemon did not start")
        info=inspect(name); front=connect(port(info,8080)); secure=connect(port(info,8081)); origin=connect(port(a,8080))
        get(front,"/label",b"a"); get(secure,"/label",b"a")
        tcp=socket.create_connection(("127.0.0.1",port(info,9090)),5); clients.append(tcp); lines=tcp.makefile("rb"); clients.append(lines)
        assert lines.readline()==b"a\n"
        held=connect(port(info,8080)); held.request("GET","/hold"); response=held.getresponse(); assert response.read(1)==b"a"
        map_host(ip_b)
        get(front,"/label",b"b"); get(secure,"/label",b"b")
        new=socket.create_connection(("127.0.0.1",port(info,9090)),5); clients.append(new); assert new.recv(2,socket.MSG_WAITALL)==b"b\n"
        tcp.sendall(b"old\n"); assert lines.readline()==b"a:old\n"
        write("a",release=True); assert response.read()==b"end"
        checks.append("DNS IP change rotates plain/TLS HTTP pools and TCP dials; existing TCP/HTTP streams stay on old backend")
        # Resolve an RFC-reserved .invalid name through a hot-applied endpoint,
        # then recover using the committed hostname.
        candidate=seed.replace("moving.test:8080","unresolved.invalid:8080")
        (out/"candidate.toml").write_text(candidate); ctl("apply","--file","/cases/candidate.toml","--if-generation","0")
        front.request("GET","/label"); bad=front.getresponse(); assert bad.status==503; bad.read()
        map_host(ip_a)
        ctl("apply","--file","/cases/seed.toml","--if-generation","1")
        get(front,"/label",b"a")
        checks.append("negative DNS/bad address fails boundedly; hot apply recovers without worker replacement")
        for path in ["/cache/etag","/cache/lm","/cache/strict"]: get(front,path,b"body-1")
        count=len(calls(origin)); get(front,"/cache/etag",b"body-1"); assert len(calls(origin))==count
        time.sleep(2.1)
        for path in ["/cache/etag","/cache/lm"]: assert get(front,path,b"body-1")["x-validation"]=="yes"
        seen=calls(origin)
        assert any(c["etag"]=='"v1"' for c in seen) and any(c["modified"] for c in seen)
        write("a",version=2); time.sleep(2.1); get(front,"/cache/etag",b"body-2")
        time.sleep(2.1); write("a",status=503)
        get(front,"/cache/etag",b"body-2"); get(front,"/cache/strict",b"body-2",status=503)
        status=ctl("cache","status","web")["caches"]["web"]["status"]
        assert status["revalidations"]>=2 and status["stale_hits"]>=1
        ctl("cache","purge","web","--all","--if-generation","2")
        get(front,"/cache/etag",b"body-2",status=503)
        checks.append("origin counters prove cache hits, ETag/Last-Modified 304, 200 replacement, bounded stale-on-error and strict/purge rejection")
        write("a",status=200)
        front.close();secure.close();run("docker","restart",name,stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                generation=ctl("status")["generation"]
                if generation==2:break
            except (subprocess.CalledProcessError,json.JSONDecodeError):pass
            time.sleep(.1)
        else: raise RuntimeError("restart recovery failed")
        info=inspect(name); front=connect(port(info,8080)); get(front,"/cache/etag",b"body-2")
        assert ctl("cache","status","web")["caches"]["web"]["status"]["revalidations"]==0
        checks.append("restart reads committed DNS/cache policy and starts new ephemeral cache/resolver state")
        summary={"state":"passed","image_id":info["Image"],"checks":checks,"cache_before_purge":status,"origin_calls":calls(origin),"method":"functional assertions; not a throughput benchmark"}
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n"); print(json.dumps(summary,indent=2),flush=True)
    finally:
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for client in clients: client.close()
        for container in reversed(created):
            with (out/f"{container}.log").open("w") as log: subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)

if __name__=="__main__": main()
