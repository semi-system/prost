"""Generate local benchmark certificates and equivalent proxy configs in ignored state."""
from pathlib import Path
import argparse
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--nginx-buffer-kib", type=int, choices=[4, 32, 64, 128], default=64)
args = parser.parse_args()

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "test-results/advanced-fixtures"
certs = OUT / "certs"
certs.mkdir(parents=True, exist_ok=True)
valid = (certs / "cert.pem").exists() and subprocess.run(
    ["openssl", "x509", "-checkend", "86400", "-noout", "-in", str(certs / "cert.pem")],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
if not valid:
    subprocess.run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
                    "-nodes", "-days", "7", "-subj", "/CN=backend",
                    "-addext", "subjectAltName=DNS:backend,DNS:localhost,IP:127.0.0.1",
                    "-addext", "basicConstraints=critical,CA:FALSE",
                    "-keyout", str(certs / "key.pem"), "-out", str(certs / "cert.pem")], check=True)
    # Test key only; both non-root fixtures need to read the same disposable key.
    (certs / "key.pem").chmod(0o644)
pattern = b"0123456789abcdef"
(OUT / "upload.bin").write_bytes(pattern * (1048576 // len(pattern)))
(OUT / "grpc-request.bin").write_bytes(bytes(5))

prost = '''schema_version = 1
[runtime]
admin_socket = "/run/prost/admin.sock"
artifact_dir = "/var/lib/prost/artifacts"
drain_seconds = 5
io_workers = 2
'''
for name, port, tls, h2 in [("plain", 8080, False, False), ("secure", 8443, True, False), ("grpc", 8443, True, True)]:
    prost += f'''
[upstreams.{name}]
http2 = {str(h2).lower()}
pool_max_idle_per_host = 128
http1_max_buf_size = 65536
health_interval_ms = 5000
[upstreams.{name}.health_check]
path = "/ready"
host = "backend"
fall = 2
rise = 2
timeout_ms = 1000
[upstreams.{name}.http2_settings]
initial_stream_window_size = 1048576
initial_connection_window_size = 8388608
'''
    if tls:
        prost += f'[upstreams.{name}.tls]\nserver_name = "backend"\nca_certificate = "/certs/cert.pem"\n'
    prost += f'[[upstreams.{name}.endpoints]]\nid = "one"\naddress = "backend:{port}"\n'

for name, port, upstream in [("plain", 8080, "plain"), ("tls", 8443, "plain"), ("double_tls", 8444, "secure")]:
    prost += f'''
[[listeners]]
id = "{name}"
address = "0.0.0.0:{port}"
protocol = "http"
upstream = "{upstream}"
'''
    if port != 8080:
        prost += '[listeners.tls]\ncertificate = "/certs/cert.pem"\nprivate_key = "/certs/key.pem"\n'
    prost += '''[listeners.http2_settings]
initial_stream_window_size = 1048576
initial_connection_window_size = 8388608
max_concurrent_streams = 128
'''
    if name == "tls":
        prost += '[[listeners.routes]]\npath_prefix = "/grpc"\npriority = 10\nupstream = "grpc"\n'
        prost += '[[listeners.routes]]\npath_prefix = "/wasm"\npriority = 100\nupstream = "plain"\n[[listeners.routes.plugins]]\nname = "wasm"\nmodule = "/plugins/add-header.wat"\nhooks = ["response_headers"]\n'
        prost += '[[listeners.routes]]\npath_prefix = "/native"\npriority = 100\nupstream = "plain"\n[[listeners.routes.plugins]]\nname = "headers"\nconfig = { response = { set = { x-prost-plugin = "wasm" } } }\n'
        prost += '[[listeners.routes]]\npath_prefix = "/chain"\npriority = 100\nupstream = "plain"\n[[listeners.routes.plugins]]\nname = "ip_acl"\nconfig = { deny = ["192.0.2.0/24"] }\n[[listeners.routes.plugins]]\nname = "rate_limit"\nconfig = { requests_per_second = 1000000, burst = 1000000 }\n[[listeners.routes.plugins]]\nname = "headers"\nconfig = { response = { set = { x-prost-plugin = "wasm" } } }\n'
        prost += '''[[listeners.routes]]
path_prefix = "/full"
priority = 200
upstream = "plain"
[[listeners.routes.plugins]]
name = "headers"
config = { request = { set = { x-test-plugin = "$request_id" } }, response = { add = [ {name="x-prost-plugin",value="wasm",always=true}, {name="x-request-id",value="$request_id",always=true}, {name="x-seen-id",value="$upstream_http_x_seen_plugin"}, {name="x-status",value="$status",always=true}, {name="x-content-type",value="$sent_http_content_type"}, {name="x-client-agent",value="$http_user_agent"} ] }, expires = "1h" }
[[listeners.routes]]
path_prefix = "/native-trailer"
priority = 200
upstream = "plain"
[[listeners.routes.plugins]]
name = "headers"
config = { response = { set = { x-prost-plugin = "wasm" } }, trailers = [ {name="x-end",value="done",always=true} ] }
[[listeners.routes]]
path_prefix = "/map"
priority = 200
upstream = "plain"
[[listeners.routes.plugins]]
name = "headers"
config = { maps = { matched_origin = { source = "$http_origin", default = "", entries = [{ pattern = "https://app.example", value = "$http_origin" }, { kind = "regex", pattern = '^https://[a-z]+[.]example$', value = "$http_origin" }] }, map_ttl = { source = "$matched_origin", default = "1h", entries = [{ pattern = "", value = "off" }] } }, request = { set = { x-test-plugin = "$matched_origin" } }, response = { add = [{ name = "access-control-allow-origin", value = "$matched_origin", always = true, when = { source = "$matched_origin", not_empty = true } }, { name = "x-seen-origin", value = "$upstream_http_x_seen_plugin" }] }, expires = "$map_ttl" }
[[listeners.routes]]
path_prefix = "/keyed"
priority = 200
upstream = "plain"
[[listeners.routes.plugins]]
name = "rate_limit"
config = { requests_per_second = 1000000, burst = 1000000, key = "$http_x_api_key", max_keys = 4096, idle_timeout_ms = 60000 }
'''
        for i in range(1000):
            prost += f'[[listeners.routes]]\npath_prefix = "/route/{i:04d}/"\nupstream = "plain"\n'
(OUT / "prost.toml").write_text(prost)
(OUT / "prost-native.toml").write_text(prost.replace(
    'name = "wasm"\nmodule = "/plugins/add-header.wat"\nhooks = ["response_headers"]',
    'name = "headers"\nconfig = { response = { set = { x-prost-plugin = "wasm" } } }'))

nginx = '''worker_processes 2;
error_log /dev/stderr warn;
pid /tmp/nginx.pid;
events { worker_connections 4096; }
http {
  access_log off;
  sendfile off;
  client_max_body_size 16m;
  keepalive_timeout 60s;
  keepalive_requests 100000;
  http2_max_concurrent_streams 128;
  http2_body_preread_size 1m;
  limit_req_zone benchmark zone=bench_rate:1m rate=1000000r/s;
  limit_req_zone $http_x_api_key zone=keyed_rate:1m rate=1000000r/s;
  map $http_origin $matched_origin {
    default "";
    "https://app.example" $http_origin;
    ~^https://[a-z]+[.]example$ $http_origin;
  }
  map $matched_origin $map_ttl { default 1h; "" off; }
  upstream plain { server backend:8080; keepalive 64; keepalive_requests 100000; keepalive_timeout 30s; }
  upstream secure { server backend:8443; keepalive 64; keepalive_requests 100000; keepalive_timeout 30s; }
  upstream rpc { server backend:8443; keepalive 64; keepalive_requests 100000; keepalive_timeout 30s; }
  ssl_certificate /certs/cert.pem;
  ssl_certificate_key /certs/key.pem;
  ssl_protocols TLSv1.2 TLSv1.3;
  ssl_session_cache shared:SSL:1m;
  proxy_http_version 1.1;
  proxy_set_header Connection "";
  proxy_set_header Host $http_host;
  proxy_set_header X-Forwarded-For $remote_addr;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_buffering off;
  proxy_request_buffering off;
  proxy_next_upstream off;
  proxy_connect_timeout 5s;
  proxy_read_timeout 60s;
  proxy_send_timeout 60s;
  server { listen 8080 reuseport backlog=1024; location / { proxy_pass http://plain; } }
  server {
    listen 8443 ssl reuseport backlog=1024;
    http2 on;
    location / { proxy_pass http://plain; }
    location /grpc {
      grpc_pass grpcs://rpc;
      grpc_ssl_verify on;
      grpc_ssl_trusted_certificate /certs/cert.pem;
      grpc_ssl_server_name on;
      grpc_ssl_name backend;
      grpc_next_upstream off;
    }
    location /wasm { add_header x-prost-plugin wasm always; proxy_pass http://plain; }
    location /native { add_header x-prost-plugin wasm always; proxy_pass http://plain; }
    location /chain { deny 192.0.2.0/24; allow all; limit_req zone=bench_rate burst=999999 nodelay; limit_req_status 429; add_header x-prost-plugin wasm always; proxy_pass http://plain; }
    location /full {
      proxy_set_header Connection "";
      proxy_set_header Host $http_host;
      proxy_set_header X-Forwarded-For $remote_addr;
      proxy_set_header X-Forwarded-Proto $scheme;
      proxy_set_header x-test-plugin $request_id;
      add_header x-prost-plugin wasm always;
      add_header x-request-id $request_id always;
      add_header x-seen-id $upstream_http_x_seen_plugin;
      add_header x-status $status always;
      add_header x-content-type $sent_http_content_type;
      add_header x-client-agent $http_user_agent;
      expires 1h;
      proxy_pass http://plain;
    }
    location /native-trailer { add_header x-prost-plugin wasm always; add_trailer x-end done always; proxy_pass http://plain; }
    location /map {
      proxy_set_header Connection "";
      proxy_set_header Host $http_host;
      proxy_set_header X-Forwarded-For $remote_addr;
      proxy_set_header X-Forwarded-Proto $scheme;
      proxy_set_header x-test-plugin $matched_origin;
      add_header access-control-allow-origin $matched_origin always;
      add_header x-seen-origin $upstream_http_x_seen_plugin;
      expires $map_ttl;
      proxy_pass http://plain;
    }
    location /keyed { limit_req zone=keyed_rate burst=999999 nodelay; limit_req_status 429; proxy_pass http://plain; }
'''
for i in range(1000):
    nginx += f'    location /route/{i:04d}/ {{ proxy_pass http://plain; }}\n'
nginx += '''  }
  server {
    listen 8444 ssl reuseport backlog=1024;
    http2 on;
    proxy_ssl_verify on;
    proxy_ssl_trusted_certificate /certs/cert.pem;
    proxy_ssl_server_name on;
    proxy_ssl_name backend;
    location / { proxy_pass https://secure; }
  }
}
'''
nginx = nginx.replace("  proxy_buffering off;", f"  proxy_buffer_size {args.nginx_buffer_kib}k;\n  proxy_buffers 8 {args.nginx_buffer_kib}k;\n  proxy_busy_buffers_size {2*args.nginx_buffer_kib}k;\n  grpc_buffer_size {args.nginx_buffer_kib}k;\n  proxy_buffering off;")
(OUT / "nginx.conf").write_text(nginx)
print(f"Prepared fixtures in {OUT}")
