"""Choose distinct guest physical cores for repeatable local benchmark placement."""
import os
import subprocess


def partitions(backend_widths):
    raw = subprocess.run(["lscpu", "--parse=CPU,CORE,SOCKET"], capture_output=True, text=True, check=True).stdout
    available = os.sched_getaffinity(0)
    cores = {}
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        cpu, core, socket = line.split(",")
        if int(cpu) in available:
            cores.setdefault((socket, core), int(cpu))
    cpus = list(cores.values())
    required = 4 + sum(backend_widths)
    if len(cpus) < required:
        raise RuntimeError(f"CPU pinning needs {required} distinct guest cores, only {len(cpus)} available")
    backends, offset = [], 4
    for width in backend_widths:
        backends.append(",".join(map(str, cpus[offset:offset+width])))
        offset += width
    return {"client": ",".join(map(str, cpus[:2])), "proxy": ",".join(map(str, cpus[2:4])), "backends": backends}
