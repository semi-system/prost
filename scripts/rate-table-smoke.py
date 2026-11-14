"""Exercise the native rate-table CLI on a release image, using private Docker state.

Functional checks only: intentional 429/TCP rejection, clear, apply and restart.
Creates and removes only UUID containers/network; does not interrupt a soak.
"""
import argparse
import http.client
import json
from pathlib import Path
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
    parser.add_argument("--backend-image", default="prost:bundle-backend")
    parser.add_argument("--cert-dir", type=Path, default=ROOT/"test-results/advanced-fixtures/certs")
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/rate-table-smoke")
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    name = "prost-rate-table-" + uuid.uuid4().hex[:10]
    backend = name + "-backend"
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
io_workers=2
drain_seconds=1
observe_plugins=true
[upstreams.web]
health_interval_ms=60000
[[upstreams.web.endpoints]]
id="one"
address="backend:8080"
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
[[listeners.plugins]]
name="headers"
config={response={set={x-runtime-generation="0"}}}
[[listeners.routes]]
path_prefix="/"
upstream="web"
[[listeners.routes.plugins]]
name="rate_limit"
hooks=["request_headers"]
config={requests_per_second=1,burst=1,key="$http_x_tenant",max_keys=128}
[[listeners]]
id="tcp"
address="0.0.0.0:9000"
protocol="tcp"
upstream="web"
[[listeners.plugins]]
name="connection_window"
config={max_connections=2,window_ms=60000}
'''
    (out/"seed.toml").write_text(seed)
    created, clients, commands, checks = [], [], [], []

    def ctl(*command, failure=None):
        result = subprocess.run(["docker", "exec", name, "prostctl", *command], capture_output=True, text=True, timeout=30)
        commands.append({"command":list(command),"exit_code":result.returncode,"stdout":result.stdout,"stderr":result.stderr})
        if failure is not None:
            assert result.returncode != 0 and failure in result.stdout+result.stderr, commands[-1]
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

    def connection(port):
        client = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        clients.append(client)
        return client

    def get(client, tenant=None):
        headers = {"x-tenant":tenant} if tenant is not None else {}
        client.request("GET", "/bytes/34", headers=headers)
        response = client.getresponse()
        body = response.read()
        if response.status == 200:
            assert body == (b"0123456789abcdef"*3)[:34]
        return response.status, dict(response.getheaders())

    http_target = ["http", "--route", "0", "--plugin", "0"]
    tcp_target = ["tcp", "--plugin", "0"]
    show_http = lambda: ctl("rate-table", "show", *http_target, "--key", "alice")
    show_tcp = lambda: ctl("rate-table", "show", *tcp_target)
    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "1", "--memory", "128m", "-e", "GOMAXPROCS=1", "-v", f"{args.cert_dir.resolve()}:/certs:ro", args.backend_image, stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-p", "127.0.0.1::8080", "-p", "127.0.0.1::9000", args.image, stdout=subprocess.DEVNULL)
        ready()
        info = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]
        port = lambda n: int(info["NetworkSettings"]["Ports"][f"{n}/tcp"][0]["HostPort"])
        frontend = connection(port(8080))
        assert get(frontend,"alice")[0] == 200
        assert get(frontend,"alice")[0] == 429
        assert ctl("rate-table", "list")["total_tables"] == 2
        assert ctl("rate-table", "list", "--listener", "http")["total_tables"] == 1
        assert show_http()["table"]["entries"][0]["key"]["value"] == "616c696365"
        checks.append("discovery, header-key inspection and intentional HTTP 429")
        ctl("rate-table", "clear", *http_target, "--key", "alice", "--all", failure="cannot be used")
        ctl("rate-table", "clear", *http_target, failure="required")
        ctl("rate-table", "clear", *http_target, "--all", "--if-generation", "9", failure="generation_conflict")
        assert show_http()["table"]["retained_entries"] == 1
        cleared = ctl("rate-table", "clear", *http_target, "--key", "616c696365", "--key-hex")
        assert cleared["affected_entries"] == 1 and cleared["ephemeral"] and not cleared["config_changed"]
        assert cleared["generation"] == 0 and get(frontend,"alice")[0] == 200
        checks.append("explicit scope, generation conflict, hex clear and CLI generation lookup")
        first, second = connection(port(9000)), connection(port(9000))
        assert get(first)[0] == 200 and get(second)[0] == 200
        denied = connection(port(9000))
        try:
            get(denied)
        except (OSError,http.client.HTTPException):
            pass
        else:
            raise AssertionError("TCP connection window did not reject the third session")
        (out/"candidate.toml").write_text(seed.replace('x-runtime-generation="0"','x-runtime-generation="1"'))
        ctl("apply", "--file", "/cases/candidate.toml", "--if-generation", "0")
        assert show_http()["generation"] == 1 and show_http()["table"]["retained_entries"] == 1
        key = show_tcp()["table"]["entries"][0]["key"]["value"]
        ctl("rate-table", "clear", *tcp_target, "--key", key, "--if-generation", "0", failure="generation_conflict")
        ctl("rate-table", "clear", *tcp_target, "--key", key, "--if-generation", "1")
        assert get(connection(port(9000)))[0] == 200
        assert get(first)[0] == 200 and get(second)[0] == 200
        ctl("rate-table", "clear", *tcp_target, "--all", "--if-generation", "1")
        assert get(first)[0] == 200
        checks.append("hot apply preserves limiter debt; clear keeps existing TCP sessions live")
        for client in clients:
            client.close()
        run("docker", "restart", name, stdout=subprocess.DEVNULL)
        assert ready()["generation"] == 1
        info = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]
        assert show_http()["table"]["retained_entries"] == 0 and show_tcp()["table"]["retained_entries"] == 0
        status, headers = get(connection(port(8080)),"alice")
        assert status == 200 and headers["x-runtime-generation"] == "1"
        checks.append("restart restores committed config and starts fresh ephemeral limiter state")
        summary = {"state":"passed","image_id":info["Image"],"method":"functional release-image CLI smoke; no throughput measurement","checks":checks,"intentional_rejections":{"http_429":1,"tcp_window":1},"final_status":ctl("status")}
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
