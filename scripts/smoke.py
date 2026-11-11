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
ctl("validate", "--file", "/examples/native-headers.toml")
ctl("apply", "--file", "/examples/native-headers.toml")
with urllib.request.urlopen(options.http_url, timeout=3) as response:
    assert response.headers["x-proxy"] == "prost"
    assert response.headers["x-status"] == "200"
    request_id = response.headers["x-request-id"]
    assert len(request_id) == 32 and all(c in "0123456789abcdef" for c in request_id)
    assert json.load(response)["backend"] == "blue"
ctl("validate", "--file", "/examples/native-policies.toml")
ctl("apply", "--file", "/examples/native-policies.toml")
def policy_request(key, origin, expected):
    request = urllib.request.Request(options.http_url, headers={"x-api-key":key,"origin":origin})
    try:
        response = urllib.request.urlopen(request, timeout=3)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        assert response.code == expected, (response.code, expected)
        assert response.headers["x-proxy"] == "prost"
        if origin == "https://app.example":
            assert response.headers["access-control-allow-origin"] == origin
            assert response.headers["x-policy"] == "trusted-origin"
            if expected == 200:
                assert response.headers["cache-control"] == "max-age=3600"
        else:
            assert "access-control-allow-origin" not in response.headers
        response.read()
policy_request("alice", "https://app.example", 200)
policy_request("alice", "https://app.example", 200)
policy_request("alice", "https://app.example", 429)
policy_request("bob", "https://other.invalid", 200)
ctl("validate", "--file", "/examples/rewrite.toml")
ctl("apply", "--file", "/examples/rewrite.toml")
root = options.http_url.rstrip("/")
for path, expected, current in [("/api/v1/item?q=1", "/api/v2/item?q=1", "/api/v2/item"),
                                 ("/users/42?q=1", "/profiles/42?source=legacy&q=1", "/profiles/42")]:
    with urllib.request.urlopen(root + path, timeout=3) as response:
        assert json.load(response)["path"] == expected
        assert response.headers["x-original-uri"] == path
        assert response.headers["x-current-uri"] == current
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
try:
    urllib.request.build_opener(NoRedirect).open(root + "/legacy?keep=1", timeout=3)
    raise AssertionError("expected native redirect")
except urllib.error.HTTPError as error:
    assert error.code == 308
    assert error.headers["location"] == "/new?keep=1"
    assert error.headers["x-proxy"] == "prost"
    error.close()
ctl("apply", "--file", "/examples/prost.toml")
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
print("PASS: native headers/maps/conditions/keyed rate limit/rewrite/redirect, HTTP, TCP, validate, atomic apply, live session, disable/enable, restart persistence")
