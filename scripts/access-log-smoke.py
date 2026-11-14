"""Local Docker/WSL access-log ENOSPC, recovery, rotation and hot-update checks.

Requires the release image, prost:advanced-backend and h2load on the WSL host.
The 64 KiB full disk is a private tmpfs; host disks/foreign containers are untouched.
This is a correctness/resource workload, not a throughput comparison.
"""
import argparse
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
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/access-log-smoke")
    args = parser.parse_args()
    if args.requests < 1000:
        parser.error("at least 1000 requests are needed to fill the private log filesystem")
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    name = "prost-log-smoke-" + uuid.uuid4().hex[:8]
    backend = name + "-backend"
    certs = ROOT / "test-results/advanced-fixtures/certs"
    seed = '''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
metrics_address="127.0.0.1:9090"
io_workers=2
drain_seconds=2
[runtime.access_log]
path="/logs/access.jsonl"
queue_capacity=32
flush_interval_ms=10
[upstreams.main]
observe_connection_reuse=true
pool_idle_timeout_ms=1000
[[upstreams.main.endpoints]]
id="one"
address="backend:8080"
[[listeners]]
id="http"
address="0.0.0.0:8080"
protocol="http"
upstream="main"
'''
    (out / "seed.toml").write_text(seed)
    created = []
    load = None

    def ctl(*command, check=True):
        result = subprocess.run(["docker", "exec", name, "prostctl", *command], capture_output=True, text=True)
        if check:
            assert result.returncode == 0, result.stderr
            return json.loads(result.stdout)
        return result

    def ready():
        for _ in range(100):
            result = ctl("status", check=False)
            if result.returncode == 0:
                return
            time.sleep(.1)
        raise RuntimeError("proxy did not become ready")

    def namespace(*command):
        return run("nsenter", "--target", str(Container(name).pid), "--net", *command, capture_output=True)

    def read_log():
        raw = run("docker", "exec", name, "cat", "/logs/access.jsonl", capture_output=True).stdout.decode()
        return [json.loads(line) for line in raw.splitlines()]

    def resources():
        sample = Container(name)
        process = {}
        for line in Path(f"/proc/{sample.pid}/smaps_rollup").read_text().splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                if value.strip().endswith("kB"):
                    process[key] = int(value.split()[0]) * 1024
        return {"process": process, "cgroup": sample.sample(), "status": ctl("status")["result"]}

    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        created.append(backend)
        run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", "backend", "--cpus", "2", "--memory", "256m", "-e", "GOMAXPROCS=2", "-v", f"{certs}:/certs:ro", "prost:advanced-backend", stdout=subprocess.DEVNULL)
        created.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m", "--tmpfs", "/logs:rw,size=64k,mode=0700,uid=10001,gid=10001", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro", args.image, stdout=subprocess.DEVNULL)
        ready()
        baseline = resources()
        sample = Container(name)
        before = sample.sample()
        samples = []
        with (out / "traffic.txt").open("w") as log:
            load = subprocess.Popen(["nsenter", "--target", str(sample.pid), "--net", "h2load", "-n", str(args.requests), "-c", "8", "-m", "8", "-t", "2", "http://127.0.0.1:8080/bytes/34?token=secret-query"], stdout=log, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 120
            while load.poll() is None:
                assert time.monotonic() < deadline, "traffic deadline exceeded"
                samples.append(sample.sample())
                time.sleep(.02)
            assert load.returncode == 0
        after = sample.sample()
        raw = (out / "traffic.txt").read_text()
        assert re.search(rf"{args.requests} succeeded, 0 failed, 0 errored, 0 timeout", raw), raw
        assert re.search(rf"\({args.requests * 34}\) data", raw), raw
        full = resources()
        counters = full["status"]["access_log"]
        assert counters["io_errors"] >= 1 and counters["dropped_io_uncertain"] > 0, counters
        assert counters["writer_alive"] == 1 and counters["pending"] == 0 and counters["active"] == 0, counters
        assert counters["accepted"] + counters["dropped_queue_full"] == args.requests, counters
        failed = ctl("access-log", "flush", "--if-generation", "0", check=False)
        assert failed.returncode != 0
        (out / "full-disk-control.txt").write_text(failed.stderr)
        # Free only this test's private tmpfs; a failed write may leave a partial
        # trailing JSON line, so recovery starts with an empty file.
        run("docker", "exec", name, "truncate", "-s", "0", "/logs/access.jsonl")
        assert ctl("access-log", "reopen", "--if-generation", "0")["result"]["epoch"] == 1
        for _ in range(8):
            content = namespace("curl", "-fsS", "--max-time", "5", "http://127.0.0.1:8080/bytes/34?token=secret-query").stdout
            assert len(content) == 34
        ctl("access-log", "flush", "--if-generation", "0")
        recovered = read_log()
        assert len(recovered) == 8 and all(l["path"] == "/bytes/34" and l["body_bytes"] == 34 and l["outcome"] == "complete" for l in recovered), recovered
        assert "secret" not in json.dumps(recovered)
        (out / "recovered.jsonl").write_text("".join(json.dumps(line) + "\n" for line in recovered))
        run("docker", "exec", name, "mv", "/logs/access.jsonl", "/logs/rotated.jsonl")
        assert ctl("access-log", "reopen", "--if-generation", "0")["result"]["epoch"] == 2
        namespace("curl", "-fsS", "http://127.0.0.1:8080/rotated")
        ctl("access-log", "flush", "--if-generation", "0")
        rotated = read_log()
        assert len(rotated) == 1 and rotated[0]["path"] == "/rotated", rotated
        conflict = ctl("access-log", "reopen", "--if-generation", "99", check=False)
        assert conflict.returncode != 0 and "generation_conflict" in conflict.stderr
        (out / "candidate.toml").write_text(seed.replace('id="http"', 'id="http"\naccess_log=false'))
        ctl("apply", "--file", "/cases/candidate.toml", "--if-generation", "0")
        namespace("curl", "-fsS", "http://127.0.0.1:8080/quiet")
        ctl("access-log", "flush", "--if-generation", "1")
        assert read_log() == rotated
        metrics = namespace("curl", "-fsS", "http://127.0.0.1:9090/metrics").stdout.decode()
        (out / "metrics.prom").write_text(metrics)
        def metric(key):
            match = re.search(rf"^{re.escape(key)}(?:\{{[^\n]*\}})? (\d+(?:\.\d+)?)$", metrics, re.M)
            assert match, key
            return float(match[1])
        assert metric("prost_http_upstream_connections_opened_total") > 0
        assert metric('prost_http_upstream_connection_responses_total{upstream="main",usage="reused"}') > 0
        assert metric("prost_access_log_reopens_total") == 2
        time.sleep(2)
        final = resources()
        assert final["status"]["connections"] == 0 and final["status"]["live_generations"] == 1, final
        image_id = json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]["Image"]
        run("docker", "restart", name, stdout=subprocess.DEVNULL)
        ready()
        restarted = ctl("status")["result"]
        assert restarted["generation"] == 1 and not restarted["shutting_down"]
        assert len(namespace("curl", "-fsS", "http://127.0.0.1:8080/bytes/34").stdout) == 34
        ctl("access-log", "flush", "--if-generation", "1")
        assert read_log() == [], "tmpfs resets on restart, persisted flag must keep new log empty"
        summary = {"requests": args.requests, "response_body_bytes": args.requests * 34, "image_id": image_id, "method": "H2 downstream, H1 upstream, 8 connections x 8 streams, 2 proxy workers / 2 CPU / 256 MiB, private 64 KiB log tmpfs forced ENOSPC", "baseline": baseline, "disk_full": full, "final": final, "restarted": restarted, "samples": samples, "traffic_cpu_us_per_request": (after["cpu_usec"] - before["cpu_usec"]) / args.requests, "traffic_peak_working_set_mib": max(s["working_set_bytes"] for s in samples + [before, after]) / 1048576, "traffic_peak_main_fds": max(s["main_process_fds"] for s in samples + [before, after]), "checks": [f"{args.requests} requests and byte counts", "real ENOSPC does not block traffic", "bounded queue", "writer suspension and explicit recovery", "JSON fields and query omission", "explicit rotation and generation conflict", "hot disable and persisted restart", "actual transport metrics"]}
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({k: summary[k] for k in ("requests", "traffic_cpu_us_per_request", "traffic_peak_working_set_mib", "traffic_peak_main_fds", "checks")}, indent=2))
    finally:
        if load is not None and load.poll() is None:
            load.kill()
            load.wait()
        for container in reversed(created):
            subprocess.run(["docker", "rm", "-f", "-v", container], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
