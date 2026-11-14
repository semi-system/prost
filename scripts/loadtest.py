"""Dependency-free HTTP/1.1 keep-alive load test + Linux cgroup v2 sampling.

Run as root inside WSL (Docker daemon must be local). No automatic retries.
Each worker has at most one outstanding request. Outputs raw samples and JSON.
"""
import argparse
import asyncio
import collections
import datetime
import json
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import time
from urllib.parse import urlsplit


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def counters(path):
    return {key: int(value) for key, value in
            (line.split() for line in path.read_text().splitlines())}


class Container:
    def __init__(self, name):
        self.name = name
        self.info = json.loads(docker("inspect", name))[0]
        self.pid = self.info["State"]["Pid"]
        group = Path(f"/proc/{self.pid}/cgroup").read_text().strip().split("::", 1)[1]
        self.group = Path("/sys/fs/cgroup") / group.lstrip("/")

    def sample(self):
        memory = counters(self.group / "memory.stat")
        cpu = counters(self.group / "cpu.stat")
        total = int((self.group / "memory.current").read_text())
        return {"cpu_usec": cpu["usage_usec"],
                "cpu_periods": cpu.get("nr_periods", 0),
                "cpu_throttled_periods": cpu.get("nr_throttled", 0),
                "cpu_throttled_usec": cpu.get("throttled_usec", 0),
                "memory_bytes": total,
                "working_set_bytes": max(0, total - memory.get("inactive_file", 0)),
                "anon_bytes": memory.get("anon", 0),
                "tasks": int((self.group / "pids.current").read_text()),
                "main_process_fds": len(list(Path(f"/proc/{self.pid}/fd").iterdir()))}


async def response(reader):
    header = await reader.readuntil(b"\r\n\r\n")
    lines = header.split(b"\r\n")
    status = int(lines[0].split()[1])
    headers = dict(line.split(b":", 1) for line in lines[1:] if b":" in line)
    headers = {k.lower(): v.strip().lower() for k, v in headers.items()}
    if headers.get(b"transfer-encoding") == b"chunked":
        chunks = []
        while True:
            size = int((await reader.readline()).split(b";", 1)[0].strip(), 16)
            if size == 0:
                while True:
                    trailer = await reader.readline()
                    if not trailer:
                        raise EOFError("truncated chunk trailers")
                    if trailer == b"\r\n":
                        break
                break
            if size > 1024 * 1024:
                raise ValueError("response too large")
            chunks.append(await reader.readexactly(size))
            if await reader.readexactly(2) != b"\r\n":
                raise ValueError("invalid chunk")
        body = b"".join(chunks)
    else:
        size = int(headers[b"content-length"])
        if size > 1024 * 1024:
            raise ValueError("response too large")
        body = await reader.readexactly(size)
    return status, body, headers.get(b"connection") == b"close"


async def workload(args, count, concurrency):
    if args.driver == "ab":
        return await ab_workload(args, count, concurrency)
    url = urlsplit(args.url)
    if url.scheme != "http":
        raise ValueError("this harness measures plaintext HTTP only")
    path = url.path or "/"
    if url.query:
        path += "?" + url.query
    payload = (f"GET {path} HTTP/1.1\r\nHost: {url.netloc}\r\n"
               "Connection: keep-alive\r\n\r\n").encode()
    latencies, errors, statuses = [], [], collections.Counter()
    received = 0

    async def worker(index):
        nonlocal received
        reader = writer = None
        for _ in range(index, count, concurrency):
            start = time.perf_counter()
            try:
                async with asyncio.timeout(args.timeout):
                    if writer is None:
                        reader, writer = await asyncio.open_connection(url.hostname, url.port or 80)
                    writer.write(payload)
                    await writer.drain()
                    status, body, close = await response(reader)
                    statuses[status] += 1
                    received += len(body)
                    if status != 200:
                        raise ValueError(f"HTTP {status}: {body[:100]!r}")
                    parsed = json.loads(body)
                    if parsed.get("backend") != args.backend or parsed.get("path") != path:
                        raise ValueError(f"unexpected body: {body[:100]!r}")
                    latencies.append((time.perf_counter() - start) * 1000)
                    if close:
                        writer.close()
                        await writer.wait_closed()
                        reader = writer = None
            except Exception as error:
                errors.append(f"{type(error).__name__}: {error}")
                if writer is not None:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except OSError:
                        pass
                reader = writer = None
        if writer is not None:
            writer.close()
            await writer.wait_closed()

    start = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(concurrency)))
    duration = time.perf_counter() - start
    ordered = sorted(latencies)

    def percentile(p):
        return ordered[max(0, math.ceil(len(ordered) * p) - 1)] if ordered else None

    return {"requests": count, "successes": len(latencies), "failures": len(errors),
            "status_codes": dict(statuses), "error_examples": errors[:10],
            "concurrency": concurrency, "duration_seconds": duration,
            "successful_requests_per_second": len(latencies) / duration,
            "response_body_bytes": received,
            "latency_ms": {"mean": sum(latencies) / len(latencies) if latencies else None,
                           "p50": percentile(0.50), "p95": percentile(0.95),
                           "p99": percentile(0.99), "max": percentile(1)}}


async def ab_workload(args, count, concurrency):
    process = await asyncio.create_subprocess_exec(
        "ab", "-q", "-k", "-n", str(count), "-c", str(concurrency),
        "-s", str(int(args.timeout)), args.url,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    data, _ = await process.communicate()
    output = data.decode()
    if process.returncode:
        raise RuntimeError(f"ApacheBench exited {process.returncode}: {output}")

    def number(pattern, default=None):
        match = re.search(pattern, output, re.MULTILINE)
        if not match:
            if default is not None:
                return default
            raise ValueError(f"missing AB field {pattern}: {output}")
        return float(match.group(1))

    complete = int(number(r"^Complete requests:\s+(\d+)"))
    failed = int(number(r"^Failed requests:\s+(\d+)"))
    non2xx = int(number(r"^Non-2xx responses:\s+(\d+)", 0))
    if complete != count or int(number(r"^Document Length:\s+(\d+)")) != 34:
        raise ValueError(f"AB request count or payload length mismatch: {output}")
    return {"requests": count, "successes": complete-failed-non2xx,
            "failures": failed+non2xx, "status_codes": {"non_2xx": non2xx},
            "error_examples": [], "concurrency": concurrency,
            "duration_seconds": number(r"^Time taken for tests:\s+([\d.]+)"),
            "successful_requests_per_second": number(r"^Requests per second:\s+([\d.]+)"),
            "response_body_bytes": int(number(r"^HTML transferred:\s+(\d+)")),
            "keepalive_requests": int(number(r"^Keep-Alive requests:\s+(\d+)")),
            "latency_ms": {"mean": number(r"^Time per request:\s+([\d.]+)"),
                           "p50": number(r"^\s*50%\s+(\d+)"),
                           "p95": number(r"^\s*95%\s+(\d+)"),
                           "p99": number(r"^\s*99%\s+(\d+)"),
                           "max": number(r"^\s*100%\s+(\d+)")},
            "raw_ab_output": output}


async def main(args):
    containers = [Container(name) for name in args.container]
    warmup = await workload(args, args.warmup, args.warmup_concurrency or min(args.concurrency, 20))
    if warmup["failures"]:
        raise RuntimeError(f"warmup failed: {warmup}")
    await asyncio.sleep(0.5)
    baseline = {c.name: c.sample() for c in containers}
    samples = []
    sampling = True
    started = time.perf_counter()

    async def monitor():
        while sampling:
            samples.append({"seconds": time.perf_counter() - started,
                            "containers": {c.name: c.sample() for c in containers}})
            await asyncio.sleep(0.05)

    task = asyncio.create_task(monitor())
    result = await workload(args, args.requests, args.concurrency)
    measured_end = time.perf_counter()
    final = {c.name: c.sample() for c in containers}
    sampling = False
    await task
    await asyncio.sleep(2)
    after = {c.name: c.sample() for c in containers}
    resources = {}
    for c in containers:
        name = c.name
        records = [baseline[name]] + [s["containers"][name] for s in samples] + [final[name]]
        cpu_seconds = (final[name]["cpu_usec"] - baseline[name]["cpu_usec"]) / 1e6
        resources[name] = {
            "baseline": baseline[name], "after_load": final[name], "after_idle_2s": after[name],
            "sampled_peak": {key: max(r[key] for r in records)
                             for key in ["memory_bytes", "working_set_bytes", "anon_bytes", "tasks", "main_process_fds"]},
            "cpu_seconds": cpu_seconds,
            "average_cpu_percent_one_core_100": cpu_seconds / (measured_end - started) * 100,
            "cpu_microseconds_per_attempt": cpu_seconds * 1e6 / args.requests}
    report = {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "url": args.url, "profile": args.profile, "driver": args.driver,
              "protocol": "HTTP/1.0 keep-alive (ApacheBench)" if args.driver == "ab" else "HTTP/1.1 plaintext keep-alive",
              "environment": {"kernel": platform.release(), "logical_cpus": os.cpu_count(),
                              "cpu": next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "unknown"),
                              "containers": {c.name: {"image": c.info["Image"], "configured_image": c.info["Config"]["Image"],
                                                      "cpu_limit_nano": c.info["HostConfig"]["NanoCpus"],
                                                      "memory_limit_bytes": c.info["HostConfig"]["Memory"]} for c in containers}},
              "warmup": warmup, "load": result, "resources": resources,
              "sampling_interval_ms": 50, "samples": samples}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"load": result, "resources": resources, "report": str(args.output)}, indent=2))
    if result["failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8080/")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--warmup", type=int, default=200)
    parser.add_argument("--warmup-concurrency", type=int)
    parser.add_argument("--profile", default="debug MVP")
    parser.add_argument("--driver", choices=["asyncio", "ab"], default="asyncio")
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--backend", default="green")
    parser.add_argument("--container", action="append", default=[])
    parser.add_argument("--output", type=Path, default=Path("test-results/load-10000.json"))
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.warmup) < 1:
        parser.error("counts must be positive")
    asyncio.run(main(args))
