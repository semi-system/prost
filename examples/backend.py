"""Dependency-free demo: labelled HTTP responses and a TCP echo service."""
import http.server
import json
import os
import socketserver
import threading
from urllib.parse import urlsplit


class HTTP(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        payload = json.dumps({"backend": os.environ.get("LABEL", "blue"),
                              "path": self.path}).encode() + b"\n"
        status = 200
        path = urlsplit(self.path).path
        if path.startswith("/status/"):
            try:
                code = int(path.removeprefix("/status/"))
                if 400 <= code <= 599:
                    status = code
            except ValueError:
                pass
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class Echo(socketserver.BaseRequestHandler):
    def handle(self):
        while data := self.request.recv(16384):
            self.request.sendall(data)


class TCP(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


threading.Thread(target=TCP(("0.0.0.0", 9000), Echo).serve_forever,
                 daemon=True).start()
http.server.ThreadingHTTPServer(("0.0.0.0", 8080), HTTP).serve_forever()
