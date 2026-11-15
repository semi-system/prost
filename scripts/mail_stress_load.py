"""Bounded HTTP/2 load rounds alongside the mail correctness workload.

The driver shares its host with the fixtures; report contention and resource
usage rather than interpreting these numbers as independent proxy capacity.
"""
import asyncio
import collections
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time


def process_sample(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        pages = int(Path(f"/proc/{pid}/statm").read_text().split()[1])
        return {"cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "rss_bytes": pages * os.sysconf("SC_PAGE_SIZE")}
    except (FileNotFoundError, ProcessLookupError):
        return {"cpu_seconds": 0, "rss_bytes": 0}


def parse(raw, logfile, expected_bytes=None):
    match = re.search(r"requests: (\d+) total, (\d+) started, (\d+) done, (\d+) succeeded, (\d+) failed, (\d+) errored, (\d+) timeout", raw)
    rate = re.search(r"([\d.]+) req/s", raw)
    if not match or not rate:
        raise ValueError("unrecognized h2load output")
    total, started, done, succeeded, failed, errored, timeout = map(int, match.groups())
    # A 0.1ms histogram has bounded memory even over a day of requests.
    histogram, statuses, count = collections.Counter(), collections.Counter(), 0
    with logfile.open() as stream:
        for line in stream:
            parts = line.split()
            if len(parts) != 3:
                raise ValueError("invalid h2load request record")
            statuses[parts[1]] += 1
            histogram[min(600000, math.ceil(int(parts[2]) / 100))] += 1
            count += 1
    target, cumulative, p99 = math.ceil(count * .99), 0, None
    for bucket, n in sorted(histogram.items()):
        cumulative += n
        if cumulative >= target:
            p99 = bucket / 10
            break
    errors = max(failed, errored, timeout, done - succeeded,
                 sum(n for code, n in statuses.items() if code != "200"), abs(count-done))
    if done <= 0:
        errors = max(errors, 1)
    received = re.search(r"\((\d+)\) data", raw)
    received = int(received[1]) if received else None
    if expected_bytes is not None and (received is None or not done*expected_bytes <= received <= started*expected_bytes):
        errors = max(errors, 1)
    return {"total": total, "started": started, "completed": done, "succeeded": succeeded,
            "failures": errors, "unfinished_at_deadline": max(0, started-done),
            "statuses": dict(statuses), "rps": float(rate[1]), "p99_ms": p99,
            "latency_resolution_ms": .1, "latency_samples": count, "response_bytes": received}


async def exercise(args, out, port, monitors, report, deadline):
    cases = [("small", "/bytes/1", False), ("download64k", "/bytes/65536", False),
             ("download1m", "/bytes/1048576", False), ("upload1m", "/upload", True),
             ("stream1m", "/stream/1048576", False)]
    report["stress"] = {"rounds": 0, "completed": 0, "failures": 0,
                        "connections": args.stress_connections, "streams_per_connection": args.stress_streams,
                        "method": "HTTP/2 TLS, no retries; driver/backend/proxy share host; admission caps raised only in this fixture"}
    snapshots = {}
    def sample(name, monitor):
        try:
            point = monitor.sample()
            point["rss_bytes"] = process_sample(monitor.pid)["rss_bytes"]
            snapshots[name] = point
            return point
        except FileNotFoundError:
            # An intentional certificate-agent restart removes its old procfs
            # entry before Docker reports the new PID. Never mask proxy/backend loss.
            if name == "certd" and report.get("agent_restarting") and name in snapshots:
                return dict(snapshots[name])
            raise
    with tempfile.TemporaryDirectory(prefix="prost-mail-load-") as directory:
        directory = Path(directory)
        upload = directory/"upload.bin"
        upload.write_bytes(b"x"*1048576)
        index = 0
        while time.monotonic() < deadline - 2:
            label, path, post = cases[index % len(cases)]
            # Ramp up before holding the configured heavy concurrency.
            clients = min(args.stress_connections, (16, 64, args.stress_connections)[min(index, 2)])
            seconds = min(args.stress_round_seconds, max(1, int(deadline-time.monotonic())))
            logfile = directory/"requests.tsv"
            command = ["h2load", "-D", str(seconds), "-c", str(clients), "-m", str(args.stress_streams),
                       "-t", "2", "-w", "20", "-W", "23", "--connection-inactivity-timeout=30",
                       f"--log-file={logfile}", "--connect-to", f"127.0.0.1:{port}"]
            if post:
                command += ["-d", str(upload)]
            command += [f"https://admin.test:{port}{path}"]
            before = {name: sample(name, monitor) for name, monitor in monitors.items()}
            pids = {name: monitor.pid for name, monitor in monitors.items()}
            started = time.monotonic()
            process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            reading = asyncio.create_task(process.communicate())
            maximum = {name: dict(point) for name, point in before.items()}
            driver = {"cpu_seconds": 0, "rss_bytes": 0}
            try:
                while not reading.done():
                    if time.monotonic()-started > seconds+40:
                        raise TimeoutError("stress load round exceeded deadline")
                    point = process_sample(process.pid)
                    driver = {key: max(value, point[key]) for key, value in driver.items()}
                    for name, monitor in monitors.items():
                        point = sample(name, monitor)
                        for key, value in point.items():
                            maximum[name][key] = max(maximum[name][key], value)
                    await asyncio.sleep(.5)
                raw = (await reading)[0].decode(errors="replace")
                elapsed = time.monotonic()-started
                (out/f"stress-{index:05d}.stdout.txt").write_text(raw)
                size = len(json.dumps({"bytes": 1048576}, separators=(",", ":")))+1 if post else int(path.rsplit("/", 1)[1])
                result = parse(raw, logfile, size)
                resources = {}
                for name, monitor in monitors.items():
                    after = sample(name, monitor)
                    reset = pids[name] != monitor.pid
                    resources[name] = {"cpu_percent": None if reset else (after["cpu_usec"]-before[name]["cpu_usec"])/elapsed/10000,
                                       "counter_reset": reset, "peak_rss_bytes": maximum[name]["rss_bytes"],
                                       "peak_working_set_bytes": maximum[name]["working_set_bytes"],
                                       "peak_fds": maximum[name]["main_process_fds"],
                                       "peak_tasks": maximum[name]["tasks"],
                                       "throttled_usec": after["cpu_throttled_usec"]-before[name]["cpu_throttled_usec"]}
                result.update(case=label, connections=clients, streams_per_connection=args.stress_streams,
                              elapsed_seconds=elapsed, resources=resources,
                              driver={**driver, "cpu_percent": driver["cpu_seconds"]/elapsed*100})
                if process.returncode:
                    result["failures"] = max(1, result["failures"])
                with (out/"stress-rounds.jsonl").open("a") as stream:
                    stream.write(json.dumps(result)+"\n")
                report["stress"]["rounds"] += 1
                report["stress"]["completed"] += result["completed"]
                report["stress"]["failures"] += result["failures"]
                report["stress"]["latest"] = result
                if result["failures"]:
                    raise AssertionError(f"stress round failed: {result}")
            finally:
                if not reading.done():
                    (out/"stress-incomplete.json").write_text(json.dumps({
                        "case":label, "connections":clients, "elapsed_seconds":time.monotonic()-started,
                        "peak_resources":maximum, "driver":driver,
                    }, indent=2)+"\n")
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 3)
                    except TimeoutError:
                        process.kill()
                        await process.wait()
                if not reading.done():
                    reading.cancel()
                await asyncio.gather(reading, return_exceptions=True)
                logfile.unlink(missing_ok=True)
            index += 1
