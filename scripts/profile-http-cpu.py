"""Sample CPU stacks in Docker/WSL. Profiling throughput is not benchmark evidence."""
import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import shutil
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("advanced_bench", ROOT / "scripts/advanced-bench.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--images", nargs="+", default=["prost:limits-control", "prost:native"])
    p.add_argument("--case", choices=bench.CASES, default="h2_64k")
    p.add_argument("--seconds", type=int, default=10)
    p.add_argument("--pin-cpus", action="store_true")
    p.add_argument("--output-dir", type=Path, required=True)
    args = p.parse_args()
    out = args.output_dir.resolve(); out.mkdir(parents=True, exist_ok=True)
    if args.pin_cpus:
        import json
        from benchmark_cpu import partitions
        affinity = partitions([2])
        override = out / "pinning.yaml"
        override.write_text("services:\n  prost:\n    cpuset: " + json.dumps(affinity["proxy"]) + "\n  backend:\n    cpuset: " + json.dumps(affinity["backends"][0]) + "\n")
        bench.COMPOSE += ["-f", str(override)]
        original_command = bench.command
        bench.command = lambda *a, **kw: ["taskset", "-c", affinity["client"], *original_command(*a, **kw)]
        (out / "affinity.json").write_text(json.dumps(affinity, indent=2))
    os.environ["PROST_ADVANCED_CONFIG"] = "prost-native.toml"
    temporary = tempfile.TemporaryDirectory(prefix="prost-perf-")
    try:
        bench.compose("up", "-d", "backend")
        bench.compose("stop", "prost", "nginx")
        bench.ready("backend")
        for index, image in enumerate(args.images):
            os.environ["PROST_ADVANCED_IMAGE"] = image
            bench.compose("up", "-d", "--no-deps", "--force-recreate", "--renew-anon-volumes", "prost")
            bench.ready("prost")
            bench.preflight("prost", args.case)
            pid = int(subprocess.run(["docker", "inspect", "--format", "{{.State.Pid}}", bench.container_name("prost")], check=True, capture_output=True, text=True).stdout)
            # perf's ring-buffer writes need a native Linux filesystem in WSL.
            data = Path(temporary.name) / f"{index}.perf.data"
            with (out / f"{index}.record.log").open("w") as log:
                record = subprocess.Popen(["perf", "record", "--namespaces", "-e", "cpu-clock:u", "-F", "199", "--call-graph", "dwarf,8192", "-p", str(pid), "-o", str(data), "--", "sleep", str(args.seconds + 2)], stdout=log, stderr=subprocess.STDOUT)
                try:
                    time.sleep(.5)
                    with (out / f"{index}.load.txt").open("w") as stream:
                        subprocess.run(bench.command("prost", args.case, 20000, duration=args.seconds), stdout=stream, stderr=subprocess.STDOUT, check=True)
                    if record.wait(timeout=10): raise RuntimeError(f"perf failed; see {index}.record.log")
                finally:
                    if record.poll() is None: record.terminate(); record.wait()
            with (out / f"{index}.report.txt").open("w") as report:
                subprocess.run(["perf", "report", "--stdio", "--no-children", "--sort", "symbol", "--percent-limit", "0.5", "--symfs", f"/proc/{pid}/root", "-i", str(data)], stdout=report, stderr=subprocess.STDOUT, check=True)
            shutil.copyfile(data, out / data.name)
            (out / f"{index}.image.txt").write_text(image + "\n" + subprocess.run(["docker", "inspect", "--format", "{{.Image}}", bench.container_name("prost")], check=True, capture_output=True, text=True).stdout)
            bench.compose("stop", "prost")
    finally:
        try: bench.compose("stop")
        finally: temporary.cleanup()

if __name__ == "__main__": main()
