"""Sequential heavy-workload comparison, run in WSL with h2load and curl installed.

No retries. Separate project/state. Nonzero exit if any round fails, even when
h2load itself returns zero. Build images and generate fixtures before running.
"""
import argparse
import atexit
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import re
import statistics
import subprocess
import tempfile
import time

from loadtest import Container

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "test-results/advanced-fixtures"
UPLOAD = FIXTURES / "upload.bin"
GRPC_REQUEST = FIXTURES / "grpc-request.bin"
COMPOSE = ["docker", "compose", "-f", str(ROOT / "benchmarks/advanced/compose.yaml")]
CASES = {
    # route, frontend protocol, requests, connections, streams/connection, body bytes
    "h1_1m": ("/bytes/1048576", "h1", 2000, 32, 1, 1048576),
    "tls_h1_64k": ("/bytes/65536", "tls-h1", 10000, 100, 1, 65536),
    "h2_64k": ("/bytes/65536", "h2", 20000, 32, 4, 65536),
    "double_tls_h2_64k": ("/bytes/65536", "double-tls", 10000, 32, 4, 65536),
    "h2_upload_1m": ("/upload", "h2", 1000, 16, 4, 72),
    "h2_stream_256k": ("/stream", "h2", 1000, 16, 4, 262144),
    "grpc_stream": ("/grpc", "h2", 2000, 16, 4, 262224),
    "route1000_64k": ("/route/0999/item", "h2", 10000, 32, 4, 65536),
    "wasm_header_64k": ("/wasm", "h2", 10000, 4, 4, 65536),
    "native_header_64k": ("/native", "h2", 10000, 32, 4, 65536),
    "wasm_header_34": ("/wasm-small", "h2", 20000, 4, 4, 34),
    "native_header_34": ("/native-small", "h2", 20000, 32, 4, 34),
    "native_chain_34": ("/chain-small", "h2", 20000, 32, 4, 34),
    "native_full_34": ("/full-small", "h2", 20000, 32, 4, 34),
    "native_trailer_64k": ("/native-trailer", "h2", 10000, 32, 4, 65536),
    "native_map_34": ("/map-small", "h2", 20000, 32, 4, 34),
    "native_map_regex_34": ("/map-regex-small", "h2", 20000, 32, 4, 34),
    "native_keyed_rate_34": ("/keyed-small", "h2", 20000, 32, 4, 34),
    "native_rewrite_exact_34": ("/rewrite-exact", "h2", 20000, 32, 4, 34),
    "native_rewrite_prefix_34": ("/rewrite-prefix/34", "h2", 20000, 32, 4, 34),
    "native_rewrite_regex_34": ("/rewrite-regex/34", "h2", 20000, 32, 4, 34),
}


def case_headers(case):
    if case == "native_map_34":
        return ["origin: https://app.example"]
    if case == "native_map_regex_34":
        return ["origin: https://tenant.example"]
    if case == "native_keyed_rate_34":
        return ["x-api-key: benchmark"]
    return []


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def compose(*args):
    return run(*COMPOSE, *args)


def container_name(target):
    return f"prost-advanced-benchmark-{target}-1"


def url_for(target, case):
    path, protocol, *_ = CASES[case]
    if target == "backend" and case.startswith("native_rewrite_"):
        path = "/bytes/34"
    ports = {"prost": (19081, 19444, 19446), "nginx": (19082, 19445, 19447), "backend": (19080, 19443, 19443)}
    plain, tls, double = ports[target]
    return f"{'http' if protocol == 'h1' else 'https'}://127.0.0.1:{plain if protocol == 'h1' else double if protocol == 'double-tls' else tls}{path}"


def curl(target, case, headers_file=None):
    command = ["curl", "--silent", "--show-error", "--fail", "--max-time", "30", "--cacert", str(FIXTURES / "certs/cert.pem"),
               "--http1.1" if CASES[case][1] in ("h1", "tls-h1") else "--http2"]
    if case == "grpc_stream":
        command += ["-H", "content-type: application/grpc", "-H", "te: trailers", "--data-binary", f"@{GRPC_REQUEST}"]
    if case == "h2_upload_1m":
        command += ["--data-binary", f"@{FIXTURES / 'upload.bin'}"]
    if headers_file:
        command += ["-D",str(headers_file)]
    for header in case_headers(case):
        command += ["-H", header]
    return run(*command, url_for(target, case), capture_output=True).stdout


def preflight(target, case):
    with tempfile.TemporaryDirectory(prefix="prost-protocol-check-") as temp:
        header_path=Path(temp)/"headers.txt"
        data = curl(target, case,header_path)
        headers=header_path.read_bytes().lower()
    if target!="backend":
        expected=b"http/2.0" if case=="grpc_stream" else b"http/1.1"
        if b"x-upstream-http-version: "+expected not in headers:
            raise RuntimeError(f"{target}/{case}: unexpected backend protocol: {headers!r}")
    size = CASES[case][-1]
    if len(data) != size:
        raise RuntimeError(f"{target}/{case}: body size {len(data)} != {size}")
    if case == "h2_upload_1m":
        expected = b"1048576:" + hashlib.sha256((FIXTURES / "upload.bin").read_bytes()).hexdigest().encode()
    elif case == "grpc_stream":
        block = b"0123456789abcdef" * 1024
        expected = (b"\0" + len(block).to_bytes(4, "big") + block) * 16
        # Separate header dump verifies the final trailer, not just status 200.
        dump = run("curl", "--silent", "--show-error", "--fail", "--http2", "--cacert", str(FIXTURES / "certs/cert.pem"),
                   "-H", "content-type: application/grpc", "--data-binary", f"@{GRPC_REQUEST}",
                   "-D", "-", "-o", "/dev/null", url_for(target, case), capture_output=True).stdout.lower()
        if b"grpc-status: 0" not in dump:
            raise RuntimeError(f"{target}: grpc-status trailer missing")
    else:
        expected = (b"0123456789abcdef" * math.ceil(size / 16))[:size]
    if data != expected:
        raise RuntimeError(f"{target}/{case}: byte integrity failed")
    if case.startswith("native_map_") and target != "backend":
        origin = case_headers(case)[0].split(": ", 1)[1].encode()
        if any(field not in headers for field in [b"access-control-allow-origin: " + origin,
                   b"x-seen-origin: " + origin, b"cache-control: max-age=3600"]):
            raise RuntimeError(f"{target}/{case}: map/filter/expiry failed: {headers!r}")
    if case.startswith(("wasm_header", "native_header", "native_chain", "native_full", "native_trailer")) and target != "backend":
        dump = run("curl", "--silent", "--show-error", "--fail", "--http2", "--cacert", str(FIXTURES / "certs/cert.pem"),
                   "-D", "-", "-o", "/dev/null", url_for(target, case), capture_output=True).stdout.lower()
        if b"x-prost-plugin: wasm" not in dump:
            raise RuntimeError(f"{target}: plugin header missing")
        if case == "native_trailer_64k" and b"x-end: done" not in dump:
            raise RuntimeError(f"{target}: native trailer missing")
        if case == "native_full_34":
            ids = re.findall(rb"x-request-id: ([0-9a-f]{32})", dump)
            if len(ids) != 1 or b"x-seen-id: " + ids[0] not in dump or b"cache-control: max-age=3600" not in dump or b"x-status: 200" not in dump or b"x-client-agent: curl/" not in dump or b"x-content-type: application/octet-stream" not in dump:
                raise RuntimeError(f"{target}: dynamic headers/expiry failed: {dump!r}")


def command(target, case, count, logfile=None, duration=0):
    path, protocol, _, clients, streams, _ = CASES[case]
    args = ["h2load", "-n", str(count), "-c", str(min(clients, count)), "-m", str(streams), "-t", "2",
            "--connection-active-timeout=120", "--connection-inactivity-timeout=30", "-w", "20", "-W", "23"]
    if duration: args[1:3] = ["-D", str(duration)]
    if protocol in ("h1", "tls-h1"):
        args += ["--h1"]
    if case == "h2_upload_1m":
        args += ["-d", str(UPLOAD)]
    if case == "grpc_stream":
        args += ["-H", "content-type: application/grpc", "-H", "te: trailers", "-d", str(GRPC_REQUEST)]
    if logfile:
        args += [f"--log-file={logfile}"]
    for header in case_headers(case):
        args += ["-H", header]
    return args + [url_for(target, case)]


def parse(raw, count, size, log, timed=False):
    requests = re.search(r"requests: (\d+) total, (\d+) started, (\d+) done, (\d+) succeeded, (\d+) failed, (\d+) errored, (\d+) timeout", raw)
    throughput = re.search(r"([\d.]+) req/s", raw)
    data = re.search(r"\((\d+)\) data", raw)
    if not requests or not throughput or not data:
        raise RuntimeError("unrecognized h2load output; raw output preserved")
    total, started, done, succeeded, failed, errored, timeout = map(int, requests.groups())
    if timed: count = done
    failures = max(count - succeeded, failed, errored, timeout)
    failures = max(failures, abs(total-done) if timed else max(abs(count-total),abs(count-started),abs(count-done)))
    if timed and started < done: failures = max(failures, 1)
    actual_bytes = int(data[1])
    if not (count * size <= actual_bytes <= started * size if timed else actual_bytes == count * size):
        failures = max(failures, 1)
    records = [line.split() for line in log.read_text().splitlines() if len(line.split()) == 3]
    failures = max(failures, abs(count-len(records)), sum(row[1] != "200" for row in records))
    latencies = sorted(int(row[2]) / 1000 for row in records)
    p99 = latencies[min(len(latencies)-1, math.ceil(len(latencies)*0.99)-1)] if latencies else None
    return {"requests": count, "started": started, "done": done, "succeeded": succeeded, "failures": failures,
            "unfinished_at_deadline": max(0, started-done) if timed else 0,
            "response_bytes": actual_bytes, "expected_response_bytes": count*size,
            "rps": float(throughput[1]), "p99_ms": p99,
            "response_mib_per_second": float(throughput[1])*size/1048576,
            "latency_samples": len(latencies)}


def thread_cpu(pid):
    result = {}
    for task in Path(f"/proc/{pid}/task").iterdir():
        try:
            fields = (task/"stat").read_text().rsplit(")", 1)[1].split()
            name = (task/"comm").read_text().strip()
            # CPU affinity does not reserve a core. Record runnable-but-not-running
            # time so host contention is visible alongside request tail latency.
            scheduled = [int(value) for value in (task/"schedstat").read_text().split()]
            # A thread can exit between these procfs reads. Publish only a
            # complete observation, never a partially populated dictionary.
            result[task.name] = {"name": name, "ticks": int(fields[11])+int(fields[12]),
                                 **dict(zip(("run_ns", "wait_ns", "timeslices"), scheduled))}
        except (FileNotFoundError, ProcessLookupError): pass
    return result


def measure(target, case, output, scale, duration=0):
    _, _, count, _, _, size = CASES[case]
    count = max(128, int(count * scale))
    preflight(target, case)
    warm = run(*command(target, case, min(count, 256)), capture_output=True, text=True)
    if not re.search(r"0 failed, 0 errored, 0 timeout", warm.stdout):
        raise RuntimeError(f"warmup failed: {warm.stdout}")
    containers = [Container(container_name(target))]
    if target != "backend":
        containers.append(Container(container_name("backend")))
    baseline = {c.name: c.sample() for c in containers}
    threads_before = thread_cpu(containers[0].pid)
    pressure_before = Path("/proc/pressure/cpu").read_text()
    samples = []
    # h2load writes one record per request. Keep that hot file on Linux tmpfs/ext4;
    # writing each record through /mnt/* (Windows/WSL bridge) distorts throughput.
    temporary_log = tempfile.NamedTemporaryFile(prefix="prost-h2load-", suffix=".tsv", delete=False)
    temporary_log.close()
    logfile = Path(temporary_log.name)
    rawfile = output.with_suffix(".stdout.txt")
    started = time.monotonic()
    with rawfile.open("w") as stream:
        process = subprocess.Popen(command(target, case, count, logfile, duration), stdout=stream, stderr=subprocess.STDOUT)
        deadline = started + max(180, duration+30)
        while process.poll() is None:
            samples.append({"elapsed": time.monotonic()-started, "containers": {c.name: c.sample() for c in containers}})
            if time.monotonic() > deadline:
                process.kill()
                process.wait()
                raise TimeoutError(f"load generator timed out: {target}/{case}")
            time.sleep(0.05)
    elapsed = time.monotonic()-started
    final = {c.name: c.sample() for c in containers}
    threads_after = thread_cpu(containers[0].pid)
    pressure_after = Path("/proc/pressure/cpu").read_text()
    raw = rawfile.read_text()
    result = parse(raw, count, size, logfile, timed=bool(duration))
    count = result["requests"]
    if count == 0: raise RuntimeError("no completed requests")
    output.with_suffix(".requests.tsv").write_bytes(logfile.read_bytes())
    logfile.unlink()
    if process.returncode:
        result["failures"] = max(result["failures"], 1)
    resources = {}
    for c in containers:
        name = c.name
        points = [baseline[name], final[name]] + [s["containers"][name] for s in samples]
        cpu_us = final[name]["cpu_usec"]-baseline[name]["cpu_usec"]
        resources[name] = {"cpu_us_per_request": cpu_us/count, "cpu_percent": cpu_us/elapsed/10000,
                           "cpu_periods": final[name]["cpu_periods"]-baseline[name]["cpu_periods"],
                           "cpu_throttled_periods": final[name]["cpu_throttled_periods"]-baseline[name]["cpu_throttled_periods"],
                           "cpu_throttled_usec": final[name]["cpu_throttled_usec"]-baseline[name]["cpu_throttled_usec"],
                           "working_set_peak_mib": max(s["working_set_bytes"] for s in points)/1048576,
                           "anon_peak_mib": max(s["anon_bytes"] for s in points)/1048576,
                           "tasks_peak": max(s["tasks"] for s in points), "baseline": baseline[name], "final": final[name]}
    result.update({"target": target, "case": case, "command": command(target, case, count, logfile, duration),
                   "duration_seconds":duration,"thread_cpu_ticks":{tid:{"name":value["name"], **{key:value[key]-threads_before.get(tid,{}).get(key,0) for key in ("ticks", "run_ns", "wait_ns", "timeslices")}} for tid,value in threads_after.items()},
                   "host_cpu_pressure": {"before": pressure_before, "after": pressure_after},
                   "wall_seconds": elapsed, "resources": resources,
                   "images": {c.name: c.info["Image"] for c in containers},
                   "limits": {c.name: {"cpu_nano": c.info["HostConfig"]["NanoCpus"], "cpuset": c.info["HostConfig"]["CpusetCpus"], "cpu_max": (c.group/"cpu.max").read_text().strip(), "memory": c.info["HostConfig"]["Memory"]} for c in containers},
                   "samples": samples})
    output.write_text(json.dumps(result, indent=2)+"\n")
    print(f"{case} {target}: {result['rps']:.0f} req/s p99={result['p99_ms']}ms failures={result['failures']}", flush=True)
    return result


def ready(target):
    port = {"backend":19080, "prost":19081, "nginx":19082}[target]
    for _ in range(100):
        r = subprocess.run(["curl", "--silent", "--fail", "--max-time", "1", f"http://127.0.0.1:{port}/ready"], capture_output=True)
        if r.returncode == 0 and r.stdout == b"ready":
            return
        time.sleep(0.1)
    raise RuntimeError(f"{target} did not become ready")


def main():
    global UPLOAD, GRPC_REQUEST
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--cases", nargs="+", choices=CASES)
    parser.add_argument("--scale", type=float, default=1)
    parser.add_argument("--prost-config", choices=["prost.toml","prost-native.toml"], default="prost-native.toml")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "test-results/advanced")
    args = parser.parse_args()
    if args.cases is None:
        args.cases=[name for name in CASES if args.prost_config!="prost-native.toml" or not name.startswith("wasm_")]
    if args.prost_config=="prost-native.toml" and any(name.startswith("wasm_") for name in args.cases):
        parser.error("WASM cases need --prost-config prost.toml and a WASM-enabled image")
    os.environ["PROST_ADVANCED_CONFIG"] = args.prost_config
    if args.rounds < 1 or args.scale <= 0:
        parser.error("rounds and scale must be positive")
    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    # Request bodies are re-read/mapped by h2load; /mnt/* I/O also bottlenecks uploads.
    for source, variable in [(FIXTURES/"upload.bin", "UPLOAD"), (FIXTURES/"grpc-request.bin", "GRPC_REQUEST")]:
        temporary = tempfile.NamedTemporaryFile(prefix="prost-h2load-body-", delete=False)
        temporary.write(source.read_bytes())
        temporary.close()
        path = Path(temporary.name)
        atexit.register(path.unlink, missing_ok=True)
        if variable == "UPLOAD": UPLOAD = path
        else: GRPC_REQUEST = path
    configs = [args.prost_config,"nginx.conf"]
    for name in configs:
        (output/name).write_bytes((FIXTURES/name).read_bytes())
    source = hashlib.sha256()
    for path in [ROOT/"Cargo.toml", ROOT/"Cargo.lock"] + sorted((ROOT/"crates").rglob("*.rs")) + sorted((ROOT/"crates").rglob("Cargo.toml")):
        source.update(str(path.relative_to(ROOT)).encode())
        source.update(path.read_bytes())
    metadata = {"kernel": platform.release(), "source_sha256":source.hexdigest(), "versions":{},
                "cpu":next((line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
                "h2load": run("h2load", "--version", capture_output=True, text=True).stdout.strip(),
                "rounds": args.rounds, "cases": {k:CASES[k] for k in args.cases}, "scale": args.scale,
                "prost_config":args.prost_config,
                "config_sha256": {name:hashlib.sha256((FIXTURES/name).read_bytes()).hexdigest() for name in configs}}
    results = []
    try:
        compose("up", "-d", "backend")
        compose("stop", "prost", "nginx")
        ready("backend")
        for case in args.cases:
            results.append(measure("backend", case, output / f"{case}-backend-before.json", args.scale))
        for round_number in range(args.rounds):
            targets = ["prost", "nginx"] if round_number % 2 == 0 else ["nginx", "prost"]
            for target in targets:
                compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", target)
                ready(target)
                if target not in metadata["versions"]:
                    version = ["nginx","-v"] if target=="nginx" else ["prost","--version"]
                    metadata["versions"][target]=run("docker","exec",container_name(target),*version,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True).stdout.strip()
                cases = args.cases[round_number % len(args.cases):]+args.cases[:round_number % len(args.cases)]
                for case in cases:
                    results.append(measure(target, case, output / f"{case}-{target}-round-{round_number+1}.json", args.scale))
                compose("stop", target)
        for case in args.cases:
            results.append(measure("backend", case, output / f"{case}-backend-after.json", args.scale))
    finally:
        compose("stop")
    summary = {"metadata": metadata, "results": {}}
    for case in args.cases:
        summary["results"][case] = {}
        for target in ("prost", "nginx", "backend"):
            group = [r for r in results if r["case"] == case and r["target"] == target]
            key = container_name(target)
            summary["results"][case][target] = {
                "rps_median": statistics.median(r["rps"] for r in group), "rps_rounds": [r["rps"] for r in group],
                "p99_ms_median": statistics.median(r["p99_ms"] for r in group),
                "cpu_us_per_request_median": statistics.median(r["resources"][key]["cpu_us_per_request"] for r in group),
                "working_set_peak_mib": max(r["resources"][key]["working_set_peak_mib"] for r in group),
                "requests": sum(r["requests"] for r in group), "failures": sum(r["failures"] for r in group),
            }
    (output/"summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps(summary, indent=2), flush=True)
    if any(r["failures"] for r in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
