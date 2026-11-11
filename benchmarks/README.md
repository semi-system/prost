# HTTP proxy comparison

Heavy native workloads use the separate project in `advanced/`:

```sh
sudo docker build -t prost:native .
sudo docker tag prost:native prost:advanced-release
sudo python3 scripts/prepare-advanced-bench.py
sudo docker compose -f benchmarks/advanced/compose.yaml build backend
sudo python3 scripts/advanced-bench.py --rounds 3 \
  --output-dir test-results/native-heavy
sudo python3 scripts/advanced-backpressure.py --no-updates \
  --output test-results/native-slow-readers.json
sudo python3 scripts/advanced-backpressure.py \
  --output test-results/native-live-updates.json
```

Requires `nghttp2-client`, `curl`, and `openssl` in WSL. The default is a native
config and excludes WASM cases. HTTP/2 uses 32 connections × 4 streams for most
cases (128 in flight); uploads and delayed streams use 16 × 4. Both proxies get
the same client settings, 2-CPU quota, 256 MiB, pool 128, TLS certificate and
backend. Prost's upstream HTTP/1 buffer is capped at 64 KiB. nginx uses
reuseport, 64 KiB streaming buffers, 1 MiB body preread, and separate keep-alive
pools for HTTP, HTTPS and gRPC;
request/response buffering and response cache are off. Request logs and upload
payloads are kept on Linux while measuring, then reports are copied to the repo.

The suite validates byte integrity, upload hashes, dynamic header values,
expires, and native/gRPC trailers before load. Every measured request must have
the expected upstream HTTP version (asserted in preflight),
status 200 and the expected total body bytes; failures are preserved. Cgroup
metrics include all proxy processes/threads. Reports include image IDs, config
hashes, source hash, raw request timings, direct-backend controls and samples.

For an explicitly enabled WASM image, pass `--prost-config prost.toml` and select
WASM cases. Those cases have a lower concurrency to respect the instance budget;
do not present their throughput as an identical-concurrency native comparison.

Focused native-policy comparison (same setup, 360,000 measured requests per
proxy with the defaults below):

```sh
sudo python3 scripts/advanced-bench.py --rounds 3 \
  --cases native_header_34 native_full_34 native_chain_34 native_map_34 native_map_regex_34 native_keyed_rate_34 \
  --output-dir test-results/native-policies-final
```

Map cases forward a nonempty Origin header, use exact or regex matching, rewrite
an upstream request header, check the echoed value, add the allowed origin and
set dynamic expires. nginx uses its native map/headers modules for the same
result. The keyed-limit case uses one nonempty hot API key shared by all 128
in-flight requests at a very high rate/burst; it measures admission overhead,
not key-cardinality capacity or rejection throughput. nginx uses limit_req with
nodelay; no request is intentionally delayed or rejected by either proxy.
Key isolation, rejection, bounded capacity and idle cleanup are functional tests.
Saved measurements and limits: [native-policies-benchmark.md](../docs/native-policies-benchmark.md).

This isolated Compose project compares the current Prost debug and release
binaries with the official nginx and Apache HTTP Server (httpd/Apache2) images.
It does not replace or edit the normal demo's managed config.

All proxy containers and the common static backend have a 2-CPU quota and
256 MiB memory limit. Containers share a Docker bridge in WSL2. All tests use
10,000 GET requests by default (configurable with `--requests`) at concurrency 100, plaintext keep-alive, no cache,
no access log, no WASM or TLS, and the same 34-byte JSON body. The backend
itself is a separate nginx returning that fixed payload; it is not the Python
demo backend used by the earlier MVP measurement. A direct-backend control is
measured before and after. Do not compare throughput to the earlier Python test
as though only the proxy changed.

Proxy settings are comparable rather than byte-for-byte identical:

- Prost: 2 Tokio workers, configurable idle pool (now 128/host; original baseline
  used 16), 5 s connect and 60 s
  upstream timeout. Benchmark config has no plugins.
- nginx: 2 workers, HTTP/1.1 upstream keep-alive, 16 idle connections per worker,
  request/response buffering disabled, automatic upstream retry disabled.
- Optional `nginx-tuned`: 64 idle connections per worker (128 total), matching
  Prost's aggregate budget. `nginx-reuseport` additionally enables reuseport.
- Prost defaults to dedicated current-thread I/O workers and per-worker pools;
  `prost-shared` selects the shared runtime for comparison. `prost-pgo` uses the
  opt-in workload-trained image built with `scripts/build-pgo.py`.
- Apache: minimal module set, event MPM, 2 child processes with 64 threads each,
  upstream connection reuse, `smax=16` per process and 30 s TTL. Apache's
  pool/thread model differs from nginx and Prost; this is not a default
  full-module distribution installation.

Every proxy starts fresh per round. Each gets 1,000 warmup requests at concurrency
100, excluded from the measured 10,000. The harness validates status and JSON
body for every request, does not retry, and preserves failed rounds. Only one
tested proxy runs at a time. Three rounds rotate the order; no compilation or
other benchmark runs concurrently.

Measurements come from cgroup v2 and include all processes/threads in each
container. CPU 100% is one logical core. Memory is working set
(`memory.current - inactive_file`), sampled every 50 ms; short spikes can be
missed. Anonymous memory and raw samples are also saved. Main-process FD counts
are not a fair cross-server comparison because nginx/Apache use child workers.
The load generator runs on the same machine, outside proxy resource limits.
This is a short local workload, not a maximum-capacity or production claim.
Shared file pages can be charged to the first container that loads them, so
working set is a Docker/cgroup measurement, not a unique application heap size.

Two drivers are available. The default asyncio driver sends HTTP/1.1 and validates
JSON for every response, but its throughput is client-limited on this machine.
The independent ApacheBench driver (`ab -k -n 10000 -c 100`) sends HTTP/1.0 with
keep-alive, checks transfer length/errors, and reports keep-alive counts and its
raw output. Readiness checks validate expected JSON. Do not combine results from
the two protocols as one latency/throughput distribution. AB percentile precision
is one millisecond. Prefer CPU-time-per-request over short-window CPU percentages:
CFS quota boundaries/accounting can produce averages slightly above 200% during
sub-second runs despite a 2-CPU quota.

## Run in WSL

Build the base MVP image first if needed, then build release and compare:

```sh
sudo docker build -t prost:mvp .
sudo python3 scripts/build-release-benchmark.py --profile dev
sudo python3 scripts/build-release-benchmark.py
sudo python3 scripts/compare-proxies.py --rounds 3
# Independent C load generator, requires apache2-utils in WSL:
sudo python3 scripts/compare-proxies.py --rounds 3 --driver ab
# Selected proxies and a separate result directory:
sudo python3 scripts/compare-proxies.py --driver ab --targets prost-release nginx \
  --output-dir test-results/my-comparison
# PGO vs matched-pool nginx, including its reuseport variant:
sudo python3 scripts/build-pgo.py
sudo python3 scripts/compare-proxies.py --rounds 5 --requests 100000 --driver ab \
  --targets prost-pgo nginx-tuned nginx-reuseport \
  --output-dir test-results/my-pgo-comparison
```

Scripts are run from the repository root. Cargo release output and registry use
the Linux Docker volumes from normal development. Image digests/IDs, limits,
versions, latency/resource samples and summary are written under
`test-results/comparison/` and `test-results/comparison-ab/` (ignored by Git).
Competitor configs are checked in
beside this README. Each round gets fresh anonymous state volumes. The runner
stops benchmark containers afterward; the regular
demo stays running. To remove benchmark containers/networks and their disposable
anonymous volumes afterward:

```sh
sudo docker compose -f benchmarks/compose.yaml down -v
```

Configuration references:
[nginx proxy module](https://nginx.org/en/docs/http/ngx_http_proxy_module.html),
[Apache mod_proxy](https://httpd.apache.org/docs/2.4/mod/mod_proxy.html),
[Apache event MPM](https://httpd.apache.org/docs/2.4/mod/event.html).
