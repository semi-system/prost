"""Slow HTTPS readers, cancellation cleanup, and live endpoint updates under traffic."""
import argparse
import asyncio
import json
from pathlib import Path
import runpy
import ssl
import subprocess
import time

from loadtest import Container

ROOT = Path(__file__).resolve().parent.parent
BENCH = runpy.run_path(str(ROOT/"scripts/advanced-bench.py"))
compose = BENCH["compose"]
ready = BENCH["ready"]


async def ctl(*args):
    process = await asyncio.create_subprocess_exec(*BENCH["COMPOSE"], "exec", "-T", "prost", "prostctl", *args,
                                                   stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await asyncio.wait_for(process.communicate(), 20)
    if process.returncode:
        raise RuntimeError(stderr.decode())
    response = json.loads(stdout)
    if response.get("error"):
        raise RuntimeError(response)
    return response


async def ready_status(target):
    port = 19081 if target == "prost" else 19082
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        await writer.drain()
        return int((await reader.readline()).split()[1])
    finally:
        writer.close()
        await writer.wait_closed()


async def scenario(target, clients, seconds, updates_enabled=True):
    context = ssl.create_default_context(cafile=str(ROOT/"test-results/advanced-fixtures/certs/cert.pem"))
    context.set_alpn_protocols(["http/1.1"])
    port = 19444 if target == "prost" else 19445
    barrier = asyncio.Barrier(clients+1)
    start = asyncio.Event()
    received = [0]*clients
    pattern = b"0123456789abcdef"*256
    started = 0

    async def reader_task(index):
        reader, writer = await asyncio.open_connection("127.0.0.1", port, ssl=context, server_hostname="localhost", limit=32768)
        try:
            writer.write(b"GET /bytes/16777216 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            headers = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            if b" 200 " not in headers.split(b"\r\n")[0]:
                raise RuntimeError(headers)
            await asyncio.wait_for(barrier.wait(), 20)
            await start.wait()
            while time.monotonic()-started < seconds:
                data = await asyncio.wait_for(reader.readexactly(4096), 10)
                if data != pattern:
                    raise RuntimeError("slow stream was interrupted or corrupted")
                received[index] += len(data)
                await asyncio.sleep(1/16)  # 64 KiB/s per reader; body never accumulated.
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), 0.5)
            except (TimeoutError, OSError):
                writer.transport.abort()

    samples = []
    container = Container(BENCH["container_name"](target))
    events_path = container.group/"memory.events"
    events_before = events_path.read_text()
    async def sample():
        while True:
            samples.append(container.sample())
            await asyncio.sleep(0.1)

    updates = []
    async def update():
        if target != "prost" or not updates_enabled:
            return
        await asyncio.sleep(seconds/4)
        before = (await ctl("status"))["result"]
        await ctl("upstream", "disable", "plain/one", "--if-generation", str(before["generation"]))
        if await ready_status(target) != 503:
            raise RuntimeError("disabled endpoint accepted a new request")
        updates.append((await ctl("status"))["result"])
        await asyncio.sleep(seconds/4)
        await ctl("upstream", "enable", "plain/one", "--if-generation", str(before["generation"]+1))
        if await ready_status(target) != 200:
            raise RuntimeError("enabled endpoint did not recover")
        updates.append((await ctl("status"))["result"])

    readers = [asyncio.create_task(reader_task(i)) for i in range(clients)]
    monitor = asyncio.create_task(sample())
    try:
        await asyncio.wait_for(barrier.wait(), 30)
        started = time.monotonic()
        start.set()
        mutation = asyncio.create_task(update())
        await asyncio.wait_for(asyncio.gather(*readers, mutation), seconds+45)
    finally:
        for task in readers: task.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
    await asyncio.sleep(2)
    after = container.sample()
    if await ready_status(target) != 200:
        raise RuntimeError("proxy unavailable after slow clients cancelled")
    final_status = (await ctl("status"))["result"] if target == "prost" else None
    if final_status and (final_status["connections"] != 0 or final_status["live_generations"] != 1):
        raise RuntimeError(f"connections/generations retained: {final_status}")
    def oom_count(text):
        return int(dict(line.split() for line in text.splitlines()).get("oom_kill", 0))
    oom_delta = oom_count(events_path.read_text())-oom_count(events_before)
    if oom_delta:
        raise RuntimeError("container OOM")
    return {"target":target,"clients":clients,"duration_seconds":seconds,"live_updates":target == "prost" and updates_enabled,"rate_bytes_per_second_per_client":65536,
            "received_bytes":received,"failures":0,"oom_kills":oom_delta,
            "working_set_peak_mib":max(s["working_set_bytes"] for s in samples)/1048576,
            "anon_peak_mib":max(s["anon_bytes"] for s in samples)/1048576,
            "after_idle":after,"updates":updates,"final_status":final_status,"samples":samples,
            "image":container.info["Image"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clients", type=int, default=32)
    parser.add_argument("--seconds", type=float, default=10)
    parser.add_argument("--no-updates", action="store_true", help="same slow-reader workload for both proxies")
    parser.add_argument("--output", type=Path, default=ROOT/"test-results/advanced-backpressure.json")
    args = parser.parse_args()
    if not (1 <= args.clients <= 128 and 2 <= args.seconds <= 300):
        parser.error("clients must be 1..128 and seconds 2..300")
    results = []
    try:
        compose("up", "-d", "backend")
        ready("backend")
        compose("stop", "prost", "nginx")
        for target in ("prost", "nginx"):
            compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", target)
            ready(target)
            result = asyncio.run(scenario(target,args.clients,args.seconds,not args.no_updates))
            results.append(result)
            print(f"{target}: peak {result['working_set_peak_mib']:.2f} MiB, all slow readers healthy", flush=True)
            compose("stop", target)
    finally:
        compose("stop")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results,indent=2)+"\n")


if __name__ == "__main__":
    main()
