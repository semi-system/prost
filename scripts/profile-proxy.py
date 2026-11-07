"""Flat software CPU sampling of an isolated benchmark proxy (WSL root).

Profiles only the selected container, including its child processes/threads.
Diagnostic run, not a throughput comparison. Requires linux-perf and ab.
"""
import argparse
import json
from pathlib import Path
import runpy
import signal
import shutil
import subprocess
import tempfile
import time

ROOT=Path(__file__).resolve().parent.parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("target",choices=["prost-release","nginx"])
parser.add_argument("--image")
parser.add_argument("--output-dir",type=Path,required=True)
args=parser.parse_args()
args.output_dir.mkdir(parents=True,exist_ok=True)
bench=runpy.run_path(str(ROOT/"scripts/compare-proxies.py"))
compose=bench["compose"]
port,container,_=bench["TARGETS"][args.target]

with tempfile.TemporaryDirectory(prefix="prost-profile-") as directory:
    if args.image:
        override=Path(directory)/"override.json"
        override.write_text(json.dumps({"services":{args.target:{"image":args.image}}}))
        bench["COMPOSE"].extend(["-f",str(override)])
    try:
        compose("up","-d","backend")
        compose("stop",*bench["TARGETS"])
        compose("up","-d","--no-deps","--force-recreate","--renew-anon-volumes",args.target)
        bench["ready"](port)
        subprocess.run(["ab","-q","-k","-n","1000","-c","100",f"http://127.0.0.1:{port}/"],check=True,capture_output=True)
        pid=json.loads(subprocess.check_output(["docker","inspect",container]))[0]["State"]["Pid"]
        group=Path(f"/proc/{pid}/cgroup").read_text().strip().split("::",1)[1]
        pids=(Path("/sys/fs/cgroup")/group.lstrip("/")/"cgroup.procs").read_text().split()
        # perf mmaps its output; keep recording on Linux rather than /mnt/*.
        data=Path(directory)/"perf.data"
        with (args.output_dir/"perf-record.log").open("w") as log:
            process=subprocess.Popen(["perf","record","-e","cpu-clock","-F","499","-p",",".join(pids),"-o",str(data)],stdout=log,stderr=log)
            try:
                time.sleep(0.2)
                load=subprocess.run(["ab","-q","-k","-n","100000","-c","100",f"http://127.0.0.1:{port}/"],check=True,capture_output=True,text=True)
                (args.output_dir/"ab.txt").write_text(load.stdout)
            finally:
                process.send_signal(signal.SIGINT)
                process.wait(timeout=30)
        with (args.output_dir/"flat.txt").open("w") as output:
            subprocess.run(["perf","report","--stdio","--no-children","--percent-limit","0.5","--sort","dso,symbol","--symfs",f"/proc/{pid}/root","-i",str(data)],stdout=output,stderr=subprocess.STDOUT,check=True)
        shutil.copyfile(data,args.output_dir/"perf.data")
        print((args.output_dir/"flat.txt").read_text())
    finally:
        compose("stop")
