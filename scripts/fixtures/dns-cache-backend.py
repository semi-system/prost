"""Independent HTTP/TLS/TCP fixture for dns-cache-smoke.py; never a benchmark."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socketserver
import ssl
import threading
import time

LABEL = os.environ["BACKEND_LABEL"]
STATE = Path(f"/cases/state-{LABEL}.json")
LOCK = threading.Lock()
CALLS = []

def state():
    return json.loads(STATE.read_text())

class HTTP(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(self, *args):
        pass
    def do_GET(self):
        if self.path == "/stats":
            with LOCK:
                body = json.dumps({"calls": CALLS.copy()}).encode()
            self.send_response(200)
            self.send_header("content-length", str(len(body)))
            self.end_headers(); self.wfile.write(body)
            return
        with LOCK:
            CALLS.append({"path": self.path, "etag": self.headers.get("if-none-match"),
                          "modified": self.headers.get("if-modified-since"), "host": self.headers.get("host")})
        current = state()
        if self.path == "/hold":
            self.send_response(200); self.send_header("transfer-encoding", "chunked"); self.end_headers()
            self.wfile.write(b"1\r\n" + LABEL.encode() + b"\r\n"); self.wfile.flush()
            deadline = time.monotonic() + 60
            while not state().get("release") and time.monotonic() < deadline:
                time.sleep(.02)
            self.wfile.write(b"3\r\nend\r\n0\r\n\r\n"); self.wfile.flush()
            return
        version = current.get("version", 1)
        tag = f'"v{version}"'
        code = current.get("status", 200)
        cached = self.path.startswith("/cache/")
        if cached and code == 200 and (self.headers.get("if-none-match") == tag or
                self.path.endswith("/lm") and self.headers.get("if-modified-since")):
            code = 304
        body = b"" if code == 304 else (f"body-{version}".encode() if cached else LABEL.encode())
        self.send_response(code)
        self.send_header("content-length", str(len(body)))
        self.send_header("x-backend", LABEL)
        self.send_header("x-validation", "yes" if code == 304 else "no")
        if cached:
            self.send_header("cache-control", "public,max-age=2,stale-if-error=3" + (",must-revalidate" if self.path.endswith("/strict") else ""))
            self.send_header("last-modified" if self.path.endswith("/lm") else "etag",
                             "Wed, 21 Oct 2015 07:28:00 GMT" if self.path.endswith("/lm") else tag)
        self.end_headers()
        if body: self.wfile.write(body)

class TCP(socketserver.StreamRequestHandler):
    def handle(self):
        self.wfile.write(LABEL.encode() + b"\n"); self.wfile.flush()
        for line in self.rfile:
            self.wfile.write(LABEL.encode() + b":" + line); self.wfile.flush()

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/certs/cert.pem", "/certs/key.pem")
plain = ThreadingHTTPServer(("0.0.0.0", 8080), HTTP)
secure = ThreadingHTTPServer(("0.0.0.0", 8443), HTTP)
secure.socket = context.wrap_socket(secure.socket, server_side=True)
for server in [plain, secure]:
    threading.Thread(target=server.serve_forever, daemon=True).start()
Server(("0.0.0.0", 9090), TCP).serve_forever()
