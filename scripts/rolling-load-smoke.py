"""Local Docker/WSL functional load: 10k HTTP requests during rolling reloads.

Fresh HTTP connections exercise the shared accept queues without retrying errors.
Each reload also keeps a TCP session alive, then waits for old workers to exit.
Only this run's UUID containers/network/anonymous volumes are removed.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    kwargs.setdefault("timeout", 60)
    return subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--reloads", type=int, default=20)
    parser.add_argument("--output-dir", type=Path, default=ROOT/"test-results/rolling-load")
    args = parser.parse_args()
    assert args.requests >= args.clients > 0 and 0 < args.reloads <= 1000
    out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=False)
    name = "prost-rolling-load-"+uuid.uuid4().hex[:10]
    monitor = name+"-metrics"
    containers, commands, samples, reloads, errors = [], [], [], [], []
    stop = threading.Event(); begun = threading.Event(); lock = threading.Lock()
    completed = 0
    sampler = None
    summary = {"state":"running", "requests":args.requests, "clients":args.clients}

    def inspect():
        return json.loads(run("docker", "inspect", name, capture_output=True, text=True).stdout)[0]

    def ctl(*command):
        response = run("docker", "exec", name, "prostctl", *command, capture_output=True, text=True)
        result = json.loads(response.stdout)["result"]
        commands.append({"command":list(command), "result":result})
        return result

    def config(generation):
        label = "blue" if generation % 2 == 0 else "green"
        return f'''schema_version=1
[runtime]
admin_socket="/run/prost/admin.sock"
artifact_dir="/var/lib/prost/artifacts"
metrics_address="127.0.0.1:9090"
reload_mode="rolling"
io_workers=2
drain_seconds=3
[upstreams.web]
pool_max_idle_per_host=4
[[upstreams.web.endpoints]]
id="one"
address="{label}:8080"
[[upstreams.echo.endpoints]]
id="one"
address="blue:9000"
[[listeners]]
id="web"
address="0.0.0.0:8080"
protocol="http"
upstream="web"
[[listeners.plugins]]
name="headers"
config={{response={{set={{x-generation="{generation}"}}}}}}
[[listeners]]
id="echo"
address="0.0.0.0:9000"
protocol="tcp"
upstream="echo"
'''

    (out/"seed.toml").write_text(config(0))
    run("docker", "network", "create", name, stdout=subprocess.DEVNULL)
    try:
        for label in ["blue", "green"]:
            backend = name+"-"+label; containers.append(backend)
            run("docker", "run", "-d", "--name", backend, "--network", name, "--network-alias", label,
                "--cpus", "1", "--memory", "128m", "-e", "LABEL="+label,
                "-v", f"{ROOT/'examples/backend.py'}:/backend.py:ro", "python:3.13-alpine", "python", "/backend.py", stdout=subprocess.DEVNULL)
        containers.append(name)
        run("docker", "run", "-d", "--name", name, "--network", name, "--cpus", "2", "--memory", "256m",
            "-e", "TOKIO_WORKER_THREADS=2", "-v", f"{out}:/cases:ro", "-v", f"{out/'seed.toml'}:/etc/prost/seed.toml:ro",
            "-p", "127.0.0.1::8080", "-p", "127.0.0.1::9000", args.image, stdout=subprocess.DEVNULL)
        for _ in range(100):
            try:
                status = ctl("status")
                if all(e["healthy"] for u in status["upstreams"].values() for e in u): break
            except (subprocess.CalledProcessError, json.JSONDecodeError): pass
            time.sleep(.1)
        else: raise RuntimeError("proxy/backends did not become ready")
        info = inspect()
        ports = {p:int(info["NetworkSettings"]["Ports"][f"{p}/tcp"][0]["HostPort"]) for p in [8080,9000]}
        # Metrics deliberately bind loopback. A private helper shares only this
        # fixture's network namespace; no metrics port is exposed on the host.
        containers.append(monitor)
        run("docker", "run", "-d", "--name", monitor, "--network", "container:"+name,
            "--pid", "container:"+name,
            "--memory", "64m", "--cpus", "0.25", "python:3.13-alpine", "python", "-c", "import time; time.sleep(3600)", stdout=subprocess.DEVNULL)

        def get():
            client = HTTPConnection("127.0.0.1", ports[8080], timeout=10)
            try:
                client.request("GET", "/", headers={"host":"load.test", "connection":"close"})
                response = client.getresponse(); body = response.read()
                generation = int(response.getheader("x-generation"))
                assert response.status == 200 and 0 <= generation <= args.reloads
                assert json.loads(body)["backend"] == ("blue" if generation % 2 == 0 else "green")
            finally: client.close()

        def metrics():
            response = run("docker", "exec", monitor, "python", "-c",
                "import urllib.request,sys; sys.stdout.write(urllib.request.urlopen('http://127.0.0.1:9090/metrics',timeout=3).read().decode())",
                capture_output=True, text=True, timeout=5)
            lines = response.stdout.splitlines()
            values = {line.split()[0]:float(line.split()[1]) for line in lines if line and not line.startswith("#") and "{" not in line.split()[0]}
            values["elapsed"] = time.monotonic()-started
            return values

        def process():
            raw = run("docker", "exec", monitor, "python", "-c", "from pathlib import Path; print(Path('/proc/1/stat').read_text())", capture_output=True, text=True).stdout
            fields = raw[raw.rfind(")")+2:].split()
            return {"cpu_ticks":int(fields[11])+int(fields[12]), "threads":int(fields[17])}

        for _ in range(32): get()
        started = time.monotonic(); before = process(); baseline = metrics(); samples.append(baseline)

        def sample():
            while not stop.wait(.2):
                try: samples.append(metrics())
                except Exception as error: errors.append("metrics: "+repr(error))

        def traffic(count):
            nonlocal completed
            for _ in range(count):
                try: get()
                except Exception as error:
                    with lock: errors.append(repr(error))
                finally:
                    with lock:
                        completed += 1
                        if completed >= args.clients: begun.set()

        sampler = threading.Thread(target=sample, daemon=True); sampler.start()
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            futures = [pool.submit(traffic, args.requests//args.clients+(index < args.requests%args.clients)) for index in range(args.clients)]
            assert begun.wait(15), "traffic did not begin"
            for generation in range(1, args.reloads+1):
                with socket.create_connection(("127.0.0.1", ports[9000]), 5) as held:
                    held.sendall(b"before"); assert held.recv(6, socket.MSG_WAITALL) == b"before"
                    candidate = out/f"candidate-{generation}.toml"; candidate.write_text(config(generation))
                    with lock: overlap = completed < args.requests
                    result = ctl("reload", "--file", f"/cases/{candidate.name}", "--if-generation", str(generation-1))
                    assert result["generation"] == generation
                    held.sendall(b"after"); assert held.recv(5, socket.MSG_WAITALL) == b"after"
                assert ctl("workers", "wait", "--generation", str(generation), "--timeout", "10")["drained"] is True
                reloads.append({"generation":generation, "during_traffic":overlap})
            for future in futures: future.result()
        elapsed = time.monotonic()-started
        stop.set(); sampler.join(5)
        assert not sampler.is_alive()
        final = metrics()
        for _ in range(100):
            if final["prost_connections"] == 0: break
            time.sleep(.02); final = metrics()
        samples.append(final); after = process(); status = ctl("status")
        assert completed == args.requests and not errors, errors[:10]
        assert any(item["during_traffic"] for item in reloads)
        assert status["generation"] == args.reloads and status["live_generations"] == 1
        assert status["worker_generations"]["retiring"] is None
        assert final["prost_connections"] == 0 and final["prost_worker_generations_live"] == 1
        assert final["prost_worker_generations_retired_total"] == args.reloads
        assert final["prost_worker_generations_forced_total"] == 0
        assert max(s["prost_worker_generations_live"] for s in samples) <= 2
        assert final["prost_process_open_fds"] <= baseline["prost_process_open_fds"]+16
        assert after["threads"] <= before["threads"]+8
        summary.update(state="passed", image_id=info["Image"], errors=errors, elapsed_seconds=elapsed,
            reloads=reloads, proxy_cpu_seconds=(after["cpu_ticks"]-before["cpu_ticks"])/os.sysconf("SC_CLK_TCK"),
            rss_bytes={"baseline":baseline["prost_process_resident_memory_bytes"], "peak":max(s["prost_process_resident_memory_bytes"] for s in samples), "final":final["prost_process_resident_memory_bytes"]},
            fds={"baseline":baseline["prost_process_open_fds"], "peak":max(s["prost_process_open_fds"] for s in samples), "final":final["prost_process_open_fds"]},
            threads={"baseline":before["threads"], "final":after["threads"]},
            method="functional reload/accept/resource checks on local Docker; fresh HTTP connections, no request retries; not a capacity benchmark")
        print(json.dumps(summary,indent=2),flush=True)
    except BaseException as error:
        summary.update(state="failed", errors=errors, failure=repr(error)); raise
    finally:
        stop.set()
        if sampler is not None: sampler.join(5)
        (out/"summary.json").write_text(json.dumps(summary,indent=2)+"\n")
        (out/"samples.json").write_text(json.dumps(samples,indent=2)+"\n")
        (out/"commands.json").write_text(json.dumps(commands,indent=2)+"\n")
        for container in reversed(containers):
            with (out/f"{container}.log").open("w") as log: subprocess.run(["docker","logs",container],stdout=log,stderr=subprocess.STDOUT,timeout=30)
            subprocess.run(["docker","rm","-f","-v",container],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        subprocess.run(["docker","network","rm",name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)


if __name__ == "__main__": main()
