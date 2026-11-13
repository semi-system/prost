"""Four-backend balancing comparison in Docker/WSL; run after preparing/building fixtures.

No retries/cache/logging. Same CPU/memory quotas and clients for both proxies.
Do not run builds or other benchmarks concurrently. Requires root for cgroup sampling.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import platform
from pathlib import Path
import statistics
import subprocess
import tempfile
import time

from loadtest import Container
from benchmark_cpu import partitions

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test-results/balance-fixtures"
CERT = ROOT / "test-results/advanced-fixtures/certs/cert.pem"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "benchmarks/balance/compose.yaml")]
NODES = ["node-a", "node-b", "node-c", "node-d"]
CASES = {"rr_uniform": ("round_robin", 0), "least_uniform": ("least_conn", 0),
         "rr_slow": ("round_robin", 5), "least_slow": ("least_conn", 5), "hash_uniform": ("consistent_hash", 0)}
DEFAULT_CASES = list(CASES)
CASES.update({"rr_backup": ("round_robin-backup", 5), "least_backup": ("least_conn-backup", 5)})
CASES.update({"rr_passive": ("round_robin-passive", 0), "least_passive": ("least_conn-passive", 0)})
CLIENT_CPUS = None
spec = importlib.util.spec_from_file_location("advanced_bench", ROOT / "scripts/advanced-bench.py")
advanced = importlib.util.module_from_spec(spec)
spec.loader.exec_module(advanced)


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def compose(*args):
    return run(*COMPOSE, *args)


def name(service):
    return "prost-balance-benchmark-" + service + "-1"


def prepare():
    FIXTURES.mkdir(parents=True, exist_ok=True)
    for fixture in ("round_robin", "least_conn", "consistent_hash", "round_robin-backup", "least_conn-backup", "round_robin-passive", "least_conn-passive"):
        algorithm = fixture.removesuffix("-backup").removesuffix("-passive")
        backup = fixture.endswith("-backup")
        passive = fixture.endswith("-passive")
        config = f'''schema_version = 1
[runtime]
admin_socket = "/run/prost/admin.sock"
artifact_dir = "/var/lib/prost/artifacts"
io_workers = 2
drain_seconds = 5
[upstreams.farm]
balance = "{algorithm}"
pool_max_idle_per_host = 128
health_interval_ms = 5000
[upstreams.farm.health_check]
path = "/ready"
timeout_ms = 1000
'''
        if algorithm == "consistent_hash":
            config = config.replace('pool_max_idle_per_host', 'hash_key = "$request_uri"\nhash_replicas = 160\npool_max_idle_per_host')
        if passive:
            config += '[upstreams.farm.passive_health]\nmax_fails = 3\nfail_timeout_ms = 1000\nfailure_statuses = [502, 503, 504]\n'
        for node in NODES:
            config += f'[[upstreams.farm.endpoints]]\nid = "{node}"\naddress = "{node}:8080"\n'
            if backup:
                config += 'max_active = 16\n' if node == "node-a" else 'backup = true\n'
        config += '''[[listeners]]
id = "https"
address = "0.0.0.0:8443"
protocol = "http"
upstream = "farm"
[listeners.tls]
certificate = "/certs/cert.pem"
private_key = "/certs/key.pem"
'''
        (FIXTURES / f"prost-{fixture}.toml").write_text(config)
        policy = "least_conn;" if algorithm == "least_conn" else "hash $request_uri consistent;" if algorithm == "consistent_hash" else ""
        servers = "\n".join(f"server {node}:8080" +
            ((" max_conns=16" if node == "node-a" else " backup") if backup else "") +
            (" max_fails=3 fail_timeout=1s" if passive else "") + ";" for node in NODES)
        nginx = f'''worker_processes 2;
error_log /dev/stderr warn;
pid /tmp/nginx.pid;
events {{ worker_connections 4096; }}
http {{
  access_log off;
  upstream farm {{ zone farm 64k; {policy} {servers}
    keepalive 256; keepalive_requests 100000; keepalive_timeout 30s;
  }}
  server {{
    listen 8443 ssl reuseport backlog=1024;
    http2 on;
    ssl_certificate /certs/cert.pem;
    ssl_certificate_key /certs/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    keepalive_timeout 60s;
    keepalive_requests 100000;
    location / {{
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_set_header Host $http_host;
      proxy_buffer_size 64k;
      proxy_buffers 8 64k;
      proxy_buffering off;
      proxy_request_buffering off;
      proxy_next_upstream off;
      proxy_pass http://farm;
    }}
  }}
}}
'''
        if passive:
            nginx = nginx.replace("proxy_next_upstream off;", "proxy_next_upstream error timeout http_502 http_503 http_504;\n      proxy_next_upstream_tries 1;")
        (FIXTURES / f"nginx-{fixture}.conf").write_text(nginx)


def backend_ips():
    ips = {}
    for node in NODES:
        info = json.loads(run("docker", "inspect", name(node), capture_output=True, text=True).stdout)[0]
        ips[node] = next(iter(info["NetworkSettings"]["Networks"].values()))["IPAddress"]
    return ips


def counts(ips):
    return {node: json.loads(run("curl", "-fsS", "--max-time", "3", f"http://{ip}:8080/stats", capture_output=True).stdout)["requests"] for node, ip in ips.items()}


def ready(ips):
    for node, ip in ips.items():
        deadline = time.monotonic() + 20
        while subprocess.run(["curl", "-sS", "--max-time", "1", f"http://{ip}:8080/ready"], capture_output=True).stdout != b"ready":
            if time.monotonic() > deadline:
                raise RuntimeError(f"backend {node} did not become ready")
            time.sleep(.1)


def preflight(target, hashing, backup=False):
    port = 19454 if target == "prost" else 19455
    for key in range(16 if hashing else 1):
        owners = []
        for _ in range(2):
            raw = run("curl", "-fsS", "--http2", "--cacert", str(CERT), "--max-time", "10", "-D", "-",
                      f"https://127.0.0.1:{port}/bytes/34?session=key-{key}", capture_output=True).stdout
            headers, body = raw.split(b"\r\n\r\n", 1)
            assert body == (b"0123456789abcdef" * 3)[:34]
            assert b"x-upstream-http-version: HTTP/1.1" in headers
            owners.append(next(line.split(b": ", 1)[1] for line in headers.splitlines() if line.lower().startswith(b"x-backend:")))
        if hashing:
            assert owners[0] == owners[1], f"{target}: affinity changed for key {key}"
        if backup:
            assert owners == [b"a", b"a"], f"{target}: backup received serial traffic with free primary"


def measure(target, case, count, output, ips):
    hashing = CASES[case][0] == "consistent_hash"
    preflight(target, hashing, case.endswith("_backup"))
    port = 19454 if target == "prost" else 19455
    with tempfile.TemporaryDirectory(prefix="prost-balance-load-") as directory:
        directory = Path(directory)
        uris = directory / "uris.txt"
        keys = range(256) if hashing else [0]
        uris.write_text("".join(f"https://127.0.0.1:{port}/bytes/34?session=key-{k}\n" for k in keys))
        def command(n, log=None):
            args = ["h2load", "-n", str(n), "-c", "32", "-m", "4", "-t", "2", "-w", "20", "-W", "23",
                    "--connection-active-timeout=120", "--connection-inactivity-timeout=30", "-i", str(uris)]
            if log: args += ["--log-file=" + str(log)]
            return ["taskset","-c",CLIENT_CPUS,*args] if CLIENT_CPUS else args
        run(*command(256), capture_output=True)
        containers = [Container(name(target))] + [Container(name(node)) for node in NODES]
        baseline = {c.name: c.sample() for c in containers}
        before = counts(ips)
        samples = []
        log = directory / "requests.tsv"
        started = time.monotonic()
        with output.with_suffix(".stdout.txt").open("w") as stream:
            process = subprocess.Popen(command(count, log), stdout=stream, stderr=subprocess.STDOUT)
            while process.poll() is None:
                samples.append({c.name:c.sample() for c in containers})
                if time.monotonic()-started > 180:
                    process.kill(); process.wait(); raise TimeoutError("balance workload timed out")
                time.sleep(.05)
        final = {c.name:c.sample() for c in containers}
        after = counts(ips)
        result = advanced.parse(output.with_suffix(".stdout.txt").read_text(), count, 34, log)
        result["backend_requests"] = {node:after[node]-before[node] for node in NODES}
        if process.returncode or sum(result["backend_requests"].values()) != count:
            result["failures"] = max(1,result["failures"])
        if case.endswith("_backup"):
            primary = result["backend_requests"]["node-a"]
            if not 0 < primary < count:
                result["failures"] = max(1, result["failures"])
        result.update({"case":case,"target":target,"images":{c.name:c.info["Image"] for c in containers},
            "limits":{c.name:{"cpu_nano":c.info["HostConfig"]["NanoCpus"],"memory":c.info["HostConfig"]["Memory"]} for c in containers},
            "resources":{},"samples":samples})
        for c in containers:
            points = [baseline[c.name],final[c.name]] + [sample[c.name] for sample in samples]
            result["resources"][c.name] = {"cpu_us_per_request":(final[c.name]["cpu_usec"]-baseline[c.name]["cpu_usec"])/count,
                "working_set_peak_mib":max(p["working_set_bytes"] for p in points)/1048576}
        output.with_suffix(".requests.tsv").write_bytes(log.read_bytes())
        output.write_text(json.dumps(result,indent=2)+"\n")
        print(f"{case} {target}: {result['rps']:.0f} req/s, p99={result['p99_ms']:.3f}ms, errors={result['failures']}, backend={result['backend_requests']}",flush=True)
        return result


def main():
    global CLIENT_CPUS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds",type=int,default=3)
    parser.add_argument("--requests",type=int,default=20000)
    parser.add_argument("--cases",nargs="+",choices=CASES,default=DEFAULT_CASES)
    parser.add_argument("--image",default="prost:native")
    parser.add_argument("--pin-cpus",action="store_true",help="Separate client/proxy/4 backends onto 8 distinct guest physical cores")
    parser.add_argument("--output-dir",type=Path,default=ROOT/"test-results/balance-final")
    args = parser.parse_args()
    if args.rounds<1 or args.requests<256: parser.error("rounds >= 1 and requests >= 256 are required")
    os.environ["PROST_BALANCE_IMAGE"]=args.image
    prepare()
    output=args.output_dir;output.mkdir(parents=True,exist_ok=True)
    affinity=partitions([1,1,1,1]) if args.pin_cpus else None
    if affinity:
        CLIENT_CPUS=affinity["client"]
        pinned={"services":{node:{"cpuset":cpu} for node,cpu in zip(NODES,affinity["backends"])}}
        for target in ("prost","nginx"): pinned["services"][target]={"cpuset":affinity["proxy"]}
        override=output.resolve()/"pinning.yaml";override.write_text(json.dumps(pinned,indent=2))
        COMPOSE.extend(["-f",str(override)])
    for file in FIXTURES.iterdir(): (output/file.name).write_bytes(file.read_bytes())
    source=hashlib.sha256()
    for path in [ROOT/"Cargo.toml",ROOT/"Cargo.lock"]+sorted((ROOT/"crates").rglob("*.rs"))+sorted((ROOT/"crates").rglob("Cargo.toml")):
        source.update(str(path.relative_to(ROOT)).encode());source.update(path.read_bytes())
    results=[]
    environment={"kernel":platform.release(),
        "cpu":next((line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),"unknown"),
        "h2load":run("h2load","--version",capture_output=True,text=True).stdout.strip(),"versions":{}}
    try:
        compose("stop")
        for round_number in range(args.rounds):
            for case in args.cases[round_number%len(args.cases):]+args.cases[:round_number%len(args.cases)]:
                algorithm,slow=CASES[case]
                os.environ["BALANCE_ALGORITHM"]=algorithm;os.environ["BALANCE_SLOW_MS"]=str(slow)
                compose("up","-d","node-b","node-c","node-d")
                compose("up","-d","--no-deps","--force-recreate","node-a")
                ips=backend_ips();ready(ips)
                targets=["prost","nginx"] if round_number%2==0 else ["nginx","prost"]
                for target in targets:
                    compose("up","-d","--no-deps","--force-recreate","--renew-anon-volumes",target)
                    for _ in range(30):
                        try: preflight(target,False);break
                        except (subprocess.CalledProcessError,AssertionError):time.sleep(.1)
                    if target not in environment["versions"]:
                        version=["nginx","-v"] if target=="nginx" else ["prost","--version"]
                        environment["versions"][target]=run("docker","exec",name(target),*version,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True).stdout.strip()
                    results.append(measure(target,case,args.requests,output/f"{case}-{target}-round-{round_number+1}.json",ips))
                    compose("stop",target)
    finally: compose("stop")
    summary={"metadata":{"source_sha256":source.hexdigest(),"rounds":args.rounds,"requests_per_case_round":args.requests,
              "client":"HTTPS/2, 32 connections x 4 streams, 2 load threads", "hash_key_corpus":256,"environment":environment,"cpu_affinity":affinity},"results":{}}
    for case in args.cases:
        summary["results"][case]={}
        for target in ("prost","nginx"):
            group=[r for r in results if r["case"]==case and r["target"]==target]; key=name(target)
            summary["results"][case][target]={"rps":statistics.median(r["rps"] for r in group),"rps_rounds":[r["rps"] for r in group],
                "p99_ms":statistics.median(r["p99_ms"] for r in group),"cpu_us":statistics.median(r["resources"][key]["cpu_us_per_request"] for r in group),
                "working_set_mib":max(r["resources"][key]["working_set_peak_mib"] for r in group),
                "requests":sum(r["requests"] for r in group),"failures":sum(r["failures"] for r in group),
                "backend_requests":{node:sum(r["backend_requests"][node] for r in group) for node in NODES}}
    (output/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
    print(json.dumps(summary,indent=2),flush=True)
    if any(r["failures"] for r in results): raise SystemExit(1)


if __name__=="__main__": main()
