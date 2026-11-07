# HTTP proxy comparison

This isolated Compose project compares the current Prost debug and release
binaries with the official nginx and Apache HTTP Server (httpd/Apache2) images.
It does not replace or edit the normal demo's managed config.

All proxy containers and the common static backend have a 2-CPU quota and
256 MiB memory limit. Containers share a Docker bridge in WSL2. All tests use
10,000 GET requests at concurrency 100, plaintext keep-alive, no cache,
no access log, no WASM or TLS, and the same 34-byte JSON body. The backend
itself is a separate nginx returning that fixed payload; it is not the Python
demo backend used by the earlier MVP measurement. A direct-backend control is
measured before and after. Do not compare throughput to the earlier Python test
as though only the proxy changed.

Proxy settings are comparable rather than byte-for-byte identical:

- Prost: 2 Tokio workers, existing 16-idle-connection pool, 5 s connect and 60 s
  upstream timeout. Benchmark config has no plugins.
- nginx: 2 workers, HTTP/1.1 upstream keep-alive, 16 idle connections per worker,
  request/response buffering disabled, automatic upstream retry disabled.
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
sudo python3 scripts/build-release-benchmark.py
sudo python3 scripts/compare-proxies.py --rounds 3
# Independent C load generator, requires apache2-utils in WSL:
sudo python3 scripts/compare-proxies.py --rounds 3 --driver ab
```

Scripts are run from the repository root. Cargo release output and registry use
the Linux Docker volumes from normal development. Image digests/IDs, limits,
versions, latency/resource samples and summary are written under
`test-results/comparison/` and `test-results/comparison-ab/` (ignored by Git).
Competitor configs are checked in
beside this README. The runner stops benchmark containers afterward; the regular
demo stays running. To remove benchmark containers/networks and their disposable
anonymous volumes afterward:

```sh
sudo docker compose -f benchmarks/compose.yaml down -v
```

Configuration references:
[nginx proxy module](https://nginx.org/en/docs/http/ngx_http_proxy_module.html),
[Apache mod_proxy](https://httpd.apache.org/docs/2.4/mod/mod_proxy.html),
[Apache event MPM](https://httpd.apache.org/docs/2.4/mod/event.html).
