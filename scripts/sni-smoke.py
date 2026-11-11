"""SNI/rotation smoke through the real daemon and CLI, in an isolated Docker project.

Run in WSL with Docker Compose v2 and openssl installed. Certificates are disposable.
"""
import argparse
from http.client import HTTPConnection, HTTPException
import json
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parent.parent


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="prost:native")
    parser.add_argument("--http-port", type=int, default=18443)
    parser.add_argument("--tcp-port", type=int, default=18444)
    parser.add_argument("--output", type=Path, default=ROOT / "test-results/sni-smoke.json")
    args = parser.parse_args()
    project = "prost-sni-smoke-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix="prost-sni-smoke-") as directory:
        directory = Path(directory)
        directory.chmod(0o755)
        fingerprints = {}
        subjects = {
            "default": ["localhost", "unknown.other.test", "example.test", "deep.shop.example.test"],
            "api": ["api.example.test", "mail.example.test"],
            "api-next": ["api.example.test", "mail.example.test"],
            "wildcard": ["*.example.test"],
        }
        for label, names in subjects.items():
            cert, key = directory / f"{label}.pem", directory / f"{label}.key"
            run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
                "-nodes", "-days", "1", "-subj", f"/CN={names[0]}",
                "-addext", "subjectAltName=" + ",".join("DNS:" + n for n in names),
                "-addext", "basicConstraints=critical,CA:FALSE",
                "-keyout", str(key), "-out", str(cert), capture_output=True)
            # Disposable test keys must be readable by the image's non-root UID.
            key.chmod(0o644)
            fingerprints[label] = ssl.PEM_cert_to_DER_cert(cert.read_text())
        ca = directory / "trust.pem"
        ca.write_text("".join((directory / f"{label}.pem").read_text() for label in subjects))
        context = ssl.create_default_context(cafile=str(ca))
        context.set_alpn_protocols(["http/1.1"])
        no_sni = ssl.create_default_context(cafile=str(ca))
        no_sni.check_hostname = False

        template = (ROOT / "examples/sni.toml").read_text()
        template = template.replace('["api.example.test"]', '["api.example.test", "mail.example.test"]')
        for name in ("web", "api"):
            template = template.replace(f"[upstreams.{name}]", f"[upstreams.{name}]\nhealth_interval_ms = 100")
        template += '''
[upstreams.echo]
health_interval_ms = 100
[[upstreams.echo.endpoints]]
id = "blue"
address = "blue:9000"
[[listeners]]
id = "mail"
address = "0.0.0.0:9443"
protocol = "tcp"
upstream = "echo"
[listeners.tls]
certificate = "/certs/default.pem"
private_key = "/certs/default.key"
reject_unknown_sni = false
[[listeners.tls.certificates]]
server_names = ["mail.example.test"]
certificate = "/certs/api.pem"
private_key = "/certs/api.key"
'''
        (directory / "base.toml").write_text(template)
        rotated = template.replace('"/certs/api.pem"', '"/certs/api-next.pem"').replace(
            '"/certs/api.key"', '"/certs/api-next.key"')
        (directory / "rotate.toml").write_text(rotated)
        (directory / "strict.toml").write_text(rotated.replace("reject_unknown_sni = false", "reject_unknown_sni = true"))
        (directory / "bad.toml").write_text(rotated.replace('"/certs/api-next.key"', '"/certs/wildcard.key"'))
        override = directory / "compose.yaml"
        override.write_text(f'''services:
  prost:
    image: {json.dumps(args.image)}
    ports: !override
      - "127.0.0.1:{args.http_port}:8443"
      - "127.0.0.1:{args.tcp_port}:9443"
    volumes:
      - {json.dumps(str(directory) + ":/certs:ro")}
      - {json.dumps(str(directory / "base.toml") + ":/etc/prost/seed.toml:ro")}
''')
        compose = ["docker", "compose", "-p", project, "-f", str(ROOT / "compose.yaml"), "-f", str(override)]

        def ctl(*arguments, succeeds=True):
            result = subprocess.run([*compose, "exec", "-T", "prost", "prostctl", *arguments],
                                    capture_output=True, text=True, timeout=20)
            if succeeds:
                if result.returncode:
                    raise RuntimeError(result.stderr)
                response = json.loads(result.stdout)
                assert response["error"] is None, response
                return response["result"]
            assert result.returncode != 0, "invalid key pair was accepted"

        def connect(name, port=args.http_port, certificate=None):
            io = socket.create_connection(("127.0.0.1", port), timeout=3)
            try:
                stream = (context if name else no_sni).wrap_socket(io, server_hostname=name)
            except BaseException:
                io.close()
                raise
            if certificate:
                assert stream.getpeercert(binary_form=True) == fingerprints[certificate]
            return stream

        def http(stream, host, backend):
            connection = HTTPConnection("127.0.0.1", args.http_port, timeout=3)
            connection.sock = stream
            connection.request("GET", "/", headers={"Host": host})
            response = connection.getresponse()
            if response.status != 200:
                response.read()
                raise HTTPException(f"expected HTTP 200 from {host}, got {response.status}")
            assert json.loads(response.read())["backend"] == backend
            # Return ownership to the caller for the next request on this TLS connection.
            connection.sock = None

        def echo(stream):
            payload = b"TLS stream survives hot update"
            stream.sendall(payload)
            received = b""
            while len(received) < len(payload):
                chunk = stream.recv(len(payload) - len(received))
                assert chunk, "TCP stream closed during update"
                received += chunk
            assert received == payload

        def rejected(name):
            try:
                stream = connect(name)
            except ssl.SSLError:
                return
            stream.close()
            raise AssertionError(f"strict SNI accepted {name!r}")

        started = time.monotonic()
        old_http = old_tcp = None
        try:
            run(*compose, "up", "-d", "--no-build")
            deadline = time.monotonic() + 20
            while True:
                try:
                    with connect("localhost", certificate="default") as stream:
                        http(stream, "localhost", "blue")
                    state = ctl("status")
                    if all(endpoint["healthy"] for endpoints in state["upstreams"].values() for endpoint in endpoints):
                        break
                    if time.monotonic() >= deadline:
                        raise RuntimeError("backends did not become healthy")
                    time.sleep(0.1)
                except (OSError, HTTPException):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
            for name, cert in [("api.example.test", "api"), ("shop.example.test", "wildcard"),
                               ("example.test", "default"), ("deep.shop.example.test", "default"),
                               ("unknown.other.test", "default"), (None, "default")]:
                with connect(name, certificate=cert) as stream:
                    http(stream, name or "localhost", "green" if cert == "api" else "blue")
            old_http = connect("api.example.test", certificate="api")
            old_tcp = connect("mail.example.test", args.tcp_port, "api")
            http(old_http, "api.example.test", "green")
            echo(old_tcp)
            generation = ctl("status")["generation"]
            ctl("validate", "--file", "/certs/rotate.toml")
            assert ctl("status")["generation"] == generation
            ctl("apply", "--file", "/certs/rotate.toml", "--if-generation", str(generation))
            http(old_http, "api.example.test", "green")
            echo(old_tcp)
            with connect("api.example.test", certificate="api-next") as stream:
                http(stream, "api.example.test", "green")
            with connect("mail.example.test", args.tcp_port, "api-next") as stream:
                echo(stream)
            state = ctl("status")
            ctl("apply", "--file", "/certs/bad.toml", "--if-generation", str(state["generation"]), succeeds=False)
            unchanged = ctl("status")
            assert (unchanged["generation"], unchanged["hash"]) == (state["generation"], state["hash"])
            ctl("apply", "--file", "/certs/strict.toml", "--if-generation", str(state["generation"]))
            rejected("unknown.other.test")
            rejected(None)
            http(old_http, "api.example.test", "green")
            echo(old_tcp)
            final = ctl("status")
            old_http.close(); old_http = None
            old_tcp.close(); old_tcp = None
            # Restart can only use the committed artifacts after all source keys/certs disappear.
            for label in subjects:
                (directory / f"{label}.pem").unlink()
                (directory / f"{label}.key").unlink()
            run(*compose, "restart", "prost")
            with connect("api.example.test", certificate="api-next") as stream:
                http(stream, "api.example.test", "green")
            with connect("mail.example.test", args.tcp_port, "api-next") as stream:
                echo(stream)
            rejected("unknown.other.test")
            assert ctl("status")["generation"] == final["generation"]
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps({"passed": True, "image": args.image,
                "image_id": run("docker", "image", "inspect", args.image, "--format", "{{.Id}}",
                                capture_output=True, text=True).stdout.strip(),
                "generation": final["generation"], "duration_seconds": time.monotonic() - started,
                "checks": ["exact/wildcard precedence", "default/no SNI", "HTTP host routing", "TCP TLS",
                    "CLI validate/apply", "certificate rotation", "live HTTP/TCP sessions", "key mismatch rollback",
                    "unknown/no SNI rejection", "artifact restart without source files"]}, indent=2) + "\n")
            print(f"PASS: SNI Docker/CLI smoke; report {args.output}", flush=True)
        except BaseException:
            subprocess.run([*compose, "logs", "--tail", "80", "prost"], check=False)
            raise
        finally:
            if old_http: old_http.close()
            if old_tcp: old_tcp.close()
            run(*compose, "down", "-v")


if __name__ == "__main__":
    main()
