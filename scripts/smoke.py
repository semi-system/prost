"""End-to-end CLI smoke against the running Docker Compose demo (run in WSL)."""
import json
import argparse
from pathlib import Path
import socket
import subprocess
import time
import urllib.request

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--compose-file",action="append")
parser.add_argument("--project-name")
parser.add_argument("--http-url",default="http://127.0.0.1:8080/")
parser.add_argument("--tcp-port",type=int,default=9000)
options=parser.parse_args()
compose=["docker","compose"]
for path in options.compose_file or [str(Path(__file__).resolve().parent.parent/"compose.yaml")]:
    compose += ["-f",path]
if options.project_name:
    compose += ["-p",options.project_name]


def ctl(*args):
    result = subprocess.run(
        [*compose, "exec", "-T", "prost", "prostctl", *args],
        check=True, text=True, capture_output=True)
    return json.loads(result.stdout)


def http_status():
    with urllib.request.urlopen(options.http_url, timeout=3) as response:
        return json.load(response)["backend"]


def wait_backend(label):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if http_status() == label:
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.1)
    raise AssertionError(f"backend did not become {label}")


ctl("apply", "--file", "/examples/prost.toml")
wait_backend("blue")
state = ctl("status")
generation = state["generation"]
ctl("validate", "--file", "/examples/green.toml")
assert ctl("status")["generation"] == generation

with socket.create_connection(("127.0.0.1", options.tcp_port), timeout=3) as old:
    old.sendall(b"before")
    assert old.recv(6) == b"before"
    result = ctl("apply", "--file", "/examples/green.toml",
                 "--if-generation", str(generation))
    assert result["generation"] == generation + 1
    old.sendall(b"after")
    assert old.recv(5) == b"after"
    wait_backend("green")

ctl("upstream", "disable", "web/green")
try:
    http_status()
    raise AssertionError("disabled upstream returned success")
except urllib.error.HTTPError as error:
    assert error.code == 503
ctl("upstream", "enable", "web/green")
wait_backend("green")

generation = ctl("status")["generation"]
subprocess.run([*compose, "restart", "prost"], check=True)
wait_backend("green")
assert ctl("status")["generation"] == generation
print("PASS: HTTP, TCP, validate, atomic apply, live session, disable/enable, restart persistence")
