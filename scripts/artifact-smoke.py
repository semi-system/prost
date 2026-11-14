"""Exercise artifact CLI/GC and durable restart during streamed traffic in Docker/WSL.

Run as root with a local Docker daemon after prepare-advanced-bench.py and the
advanced backend build. This is a correctness/resource test, not an RPS benchmark.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import uuid
from loadtest import Container

ROOT = Path(__file__).resolve().parent.parent

def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/artifact-smoke")
    args = parser.parse_args()
    if args.requests < 1000: parser.error("requests must be at least 1000 for GC/traffic overlap")
    out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=True)
    certs = ROOT / "test-results/advanced-fixtures/certs"
    name = "prost-artifact-smoke-" + uuid.uuid4().hex[:8]
    backend = name + "-backend"
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
metrics_address="127.0.0.1:9090"
io_workers=2
drain_seconds=2
[upstreams.main]
pool_idle_timeout_ms=1000
[[upstreams.main.endpoints]]
id="one"
address="backend:8080"
weight=1
[[listeners]]
id="https"
address="0.0.0.0:8443"
protocol="http"
upstream="main"
[listeners.tls]
certificate="/certs/cert.pem"
private_key="/certs/key.pem"
'''
    (out / "seed.toml").write_text(seed)
    orphan_dir = out / "orphans"; orphan_dir.mkdir(exist_ok=True)
    for n in range(100):
        data = f"unreferenced fixture {n}".encode()
        (orphan_dir / hashlib.sha256(data).hexdigest()).write_bytes(data)
    def ctl(*command, target=name):
        return json.loads(run("docker", "exec", target, "prostctl", *command, capture_output=True, text=True).stdout)
    def ready(target=name):
        for _ in range(100):
            result = subprocess.run(["docker", "exec", target, "prostctl", "status"], capture_output=True)
            if result.returncode == 0: return
            time.sleep(.1)
        raise RuntimeError("daemon did not become ready")
    def resource():
        sample = Container(name)
        process = {}
        for line in Path(f"/proc/{sample.pid}/smaps_rollup").read_text().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                if value.strip().endswith("kB"): process[key] = int(value.split()[0]) * 1024
        return {"process": process, "cgroup": sample.sample(), "status": ctl("status")["result"]}
    started = []
    load = None
    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        started.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", "prost:advanced-backend", stdout=subprocess.DEVNULL)
        started.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", "-v", f"{certs}:/certs:ro", "-p", "127.0.0.1:19887:8443", args.image, stdout=subprocess.DEVNULL)
        ready()
        before = resource()
        run("docker", "cp", str(orphan_dir) + "/.", name + ":/var/lib/prost/artifacts")
        dry = ctl("artifacts", "gc", "--min-age-seconds", "0", "--if-generation", "0")["result"]
        assert dry["dry_run"] and dry["eligible_files"] == 100 and dry["deleted_files"] == 0, dry
        observations = []
        with (out / "traffic.txt").open("w") as log:
            load = subprocess.Popen(["h2load", "--h1", "-n", str(args.requests), "-c", "32", "-t", "2", "https://127.0.0.1:19887/stream"], stdout=log, stderr=subprocess.STDOUT)
            for i in range(10):
                (out / "candidate.toml").write_text(seed.replace("weight=1", f"weight={1+i%2}"))
                ctl("apply", "--file", "/cases/candidate.toml", "--if-generation", str(i))
                overlapping = load.poll() is None
                gc = ctl("artifacts", "gc", "--delete", "--min-age-seconds", "0", "--max-delete-files", "10", "--if-generation", str(i+1))["result"]
                assert gc["deleted_files"] == 10 and gc["errors"] == 0 and gc["durable"], gc
                observations.append({"gc": gc, "load_running": overlapping, "resources": resource()})
            assert load.wait(timeout=180) == 0
        raw = (out / "traffic.txt").read_text()
        assert re.search(rf"{args.requests} succeeded, 0 failed, 0 errored, 0 timeout", raw), raw
        data = re.search(r"\((\d+)\) data", raw)
        assert data and int(data[1]) == args.requests * 262144, raw
        assert any(row["load_running"] for row in observations), "no GC overlapped traffic"
        remaining = ctl("artifacts", "gc", "--min-age-seconds", "0", "--if-generation", "10")["result"]
        assert remaining["eligible_files"] == 0 and remaining["referenced_files"] == 2, remaining
        time.sleep(2)
        final = resource()
        assert final["status"]["connections"] == 0 and final["status"]["live_generations"] == 1, final
        pid = Container(name).pid
        metrics = run("nsenter", "--target", str(pid), "--net", "curl", "-fsS", "http://127.0.0.1:9090/metrics", capture_output=True).stdout
        (out / "metrics.prom").write_bytes(metrics)
        assert b"prost_artifact_gc_deleted_files_total 100\n" in metrics
        run("docker", "restart", name, stdout=subprocess.DEVNULL); ready()
        restarted = ctl("status")["result"]
        assert restarted["generation"] == 10
        content = run("curl", "-fsS", "--max-time", "10", "--cacert", str(certs / "cert.pem"), "https://127.0.0.1:19887/bytes/34", capture_output=True).stdout
        assert content == (b"0123456789abcdef" * 3)[:34]

        # Real ENOSPC is confined to this container's 64 KiB tmpfs, never the
        # host disk or another project's volume. A partial artifact write must
        # leave the live config, TLS service and committed files usable.
        limited = name + "-full"
        full_seed = seed.replace('/var/lib/prost/artifacts', '/limited/artifacts')
        (out / "full-seed.toml").write_text(full_seed)
        (out / "full-candidate.toml").write_text(full_seed.replace('/certs/cert.pem', '/cases/oversized.pem'))
        (out / "oversized.pem").write_bytes((certs / "cert.pem").read_bytes() + b"\n" + b"x" * 131072)
        started.append(limited)
        run("docker", "run", "-d", "--name", limited, "--network", name, "--cpus", "2", "--memory", "256m", "--tmpfs", "/limited:rw,size=64k,mode=0700,uid=10001,gid=10001", "-v", f"{out}:/cases:ro", "-v", f"{out/'full-seed.toml'}:/etc/prost/seed.toml:ro", "-v", f"{certs}:/certs:ro", "-p", "127.0.0.1:19888:8443", args.image, stdout=subprocess.DEVNULL)
        ready(limited)
        failed = subprocess.run(["docker", "exec", limited, "prostctl", "apply", "--file", "/cases/full-candidate.toml", "--if-generation", "0"], capture_output=True, text=True)
        (out / "enospc.txt").write_text(failed.stdout + failed.stderr)
        assert failed.returncode != 0 and "os error 28" in failed.stderr, failed.stderr
        enospc = ctl("status", target=limited)["result"]
        assert enospc["generation"] == 0
        clean = ctl("artifacts", "gc", "--min-age-seconds", "0", target=limited)["result"]
        assert clean["eligible_files"] == 0 and clean["referenced_files"] == 2, clean
        content = run("curl", "-fsS", "--max-time", "10", "--cacert", str(certs / "cert.pem"), "https://127.0.0.1:19888/bytes/34", capture_output=True).stdout
        assert content == (b"0123456789abcdef" * 3)[:34]
        report = {"image": args.image, "image_id": json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["Image"], "requests": args.requests, "response_bytes": args.requests * 262144, "failures": 0, "gc_during_load": sum(row["load_running"] for row in observations), "deleted_files": 100, "initial": before, "observations": observations, "final": final, "restart": restarted, "enospc": {"rejected": True, "generation": enospc["generation"], "partial_write_cleanup": clean}}
        (out / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"PASS: {args.requests} streamed requests, 10 apply/GC, 100 orphan files removed, durable restart and real ENOSPC rollback", flush=True)
    finally:
        if load is not None and load.poll() is None: load.kill(); load.wait()
        for container in reversed(started):
            with (out / ("proxy.log" if container == name else "backend.log" if container == backend else "enospc-proxy.log")).open("w") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT)
            subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

if __name__ == "__main__": main()
