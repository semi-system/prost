"""Count real backend TCP accepts during identical proxy workloads.

Uses the benchmark backend's stub_status; does not change application code.
Requires the release benchmark image and ApacheBench in WSL.
"""
import http.client
import json
from pathlib import Path
import runpy
import subprocess

ROOT = Path(__file__).resolve().parent.parent
benchmark = runpy.run_path(str(ROOT / "scripts/compare-proxies.py"))
compose, ready = benchmark["compose"], benchmark["ready"]
targets = benchmark["TARGETS"]
results = []


def ab(port, count):
    completed = subprocess.run(["ab", "-q", "-k", "-n", str(count), "-c", "100",
                                f"http://127.0.0.1:{port}/"],
                               check=True, capture_output=True, text=True)
    output = completed.stdout
    if f"Complete requests:      {count}" not in output or "Failed requests:        0" not in output:
        raise RuntimeError(output)
    return output


def status(connection):
    connection.request("GET", "/__bench_status")
    response = connection.getresponse()
    text = response.read().decode()
    if response.status != 200:
        raise RuntimeError(text)
    accepted, handled, requests = map(int, text.splitlines()[2].split())
    return {"accepted": accepted, "handled": handled, "requests": requests}


try:
    compose("up", "-d", "backend")
    compose("stop", *targets)
    ready(18090)
    for round_number in range(3):
        for name in ("prost-release", "nginx"):
            port, _, _ = targets[name]
            compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", name)
            ready(port)
            ab(port, 1000)
            monitor = http.client.HTTPConnection("127.0.0.1", 18090, timeout=5)
            before = status(monitor)
            raw = ab(port, 10000)
            after = status(monitor)
            monitor.close()
            result = {"proxy": name, "round": round_number + 1,
                      "backend_new_tcp_connections": after["accepted"] - before["accepted"],
                      "backend_requests_delta": after["requests"] - before["requests"],
                      "before": before, "after": after, "raw_ab_output": raw}
            results.append(result)
            print(json.dumps({k: v for k, v in result.items() if k != "raw_ab_output"}), flush=True)
            compose("stop", name)
    directory = ROOT / "test-results/comparison"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "upstream-reuse.json").write_text(json.dumps(results, indent=2) + "\n")
finally:
    compose("stop")
