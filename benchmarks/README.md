# HTTP proxy comparison

Memory/observability profiling and fixed/timed image controls:
[memory-profiling.md](../docs/memory-profiling.md),
[observability.md](../docs/observability.md).

Four-backend algorithm comparison (independent project):

```sh
sudo python3 scripts/prepare-advanced-bench.py
sudo docker compose -f benchmarks/advanced/compose.yaml build backend
sudo python3 scripts/balance-bench.py --image prost:native --rounds 3 --requests 20000
```

Compares weighted round-robin and least-conn with uniform backends and with one
backend delaying response headers by 5 ms; consistent hash uses a uniform farm
and a corpus of 256 request-URI keys. Proxy quota is 2 CPU/256 MiB, each of four
backends gets 1 CPU/128 MiB. All cases use TLS + HTTP/2 downstream, HTTP/1.1
upstream, 34-byte bodies, 32 connections × 4 streams and 2 h2load threads.
Each proxy runs alone; image and case order rotate. Request logging, cache,
buffering and retries are off. nginx uses a shared upstream zone for global
least-conn state. Prost runs active /ready checks, which do not increment the
fixture's measured request counters; nginx uses passive checking.

Idle capacity is capped at 512 across the four addresses: Prost allows 128 per
host, nginx 256 per worker for the group. Allocation differs (per host versus
shared group), so pool behavior remains part of the result. Backend request
counters must sum to the measured load, validating that responses reach the
backend. Key affinity is checked in preflight; nginx and Prost hash mappings
differ. Compare hash results as complete routing workloads, not isolated hash
function cost. Raw counts expose backend distribution. Client/server share the
WSL host and are not CPU-pinned; CI's small functional runs are not performance
measurements. Do not run another benchmark/build concurrently.
Pass `--pin-cpus` on a host with at least 8 distinct guest cores to place the
2-thread client, 2-core proxy and four backends on separate cores. Pinning uses
one logical CPU per core from lscpu and records the masks. It does not isolate
Windows/hypervisor work. Saved results: [load-balancing-benchmark.md](../docs/load-balancing-benchmark.md).

Endpoint capacity and backup spillover (optional cases, not in the five-case default):

```sh
sudo python3 scripts/balance-bench.py --pin-cpus --rounds 3 --requests 20000 \
  --cases rr_backup least_backup --output-dir test-results/capacity-benchmark-final
```

node-a is the sole primary, with a 5 ms delay and cap 16; the other three nodes
are backups without separate caps. Prost uses `max_active`, nginx `max_conns`
with a shared zone and HTTP/1 upstream. Preflight serial traffic must stay on
the primary; measured concurrent traffic must reach both tiers, with every
request accounted for by backend counters. Units differ for multiplexed HTTP/2
upstreams. See [upstream-capacity.md](../docs/upstream-capacity.md).
Saved measurements: [upstream-capacity-benchmark.md](../docs/upstream-capacity-benchmark.md).
The control requires the preserved pre-change image `prost:admission-control`
(image ID in the saved summaries), not a rebuild of current source with another tag.

```sh
sudo python3 scripts/compare-header-images.py --before prost:admission-control \
  --after prost:native --rounds 5 --pin-cpus --output-dir test-results/capacity-header-final
sudo python3 scripts/report-balance-bench.py --directory test-results/capacity-benchmark-final \
  --control test-results/capacity-header-final/summary.json \
  --previous-control test-results/capacity-header-control/summary.json \
  --date 2026-11-13 --output docs/upstream-capacity-benchmark.md
```

Passive policy on healthy traffic (optional cases):

```sh
sudo python3 scripts/balance-bench.py --pin-cpus --rounds 3 --requests 20000 \
  --cases rr_passive least_passive --output-dir test-results/passive-benchmark
sudo python3 scripts/compare-header-images.py --before prost:passive-control \
  --after prost:native --rounds 5 --pin-cpus --output-dir test-results/passive-header-control
sudo python3 scripts/report-balance-bench.py --directory test-results/passive-benchmark \
  --control test-results/passive-header-control/summary.json \
  --date 2026-11-13 --output docs/passive-health-benchmark.md
```

The control image is the preserved binary before passive policy support. Four
healthy backends return 200, while both proxies configure threshold 3, timeout 1s,
and failure statuses 502/503/504. nginx uses a shared zone and one upstream try;
backend counters must account for all requests without replay. This measures the
healthy path with policy enabled, not fault/recovery speed or identical health
semantics. Protocol/fault tests and the CLI demo exercise failure paths separately.
Saved results: [passive-health-benchmark.md](../docs/passive-health-benchmark.md).

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

Native rewrite comparison, including unchanged literal/full-header controls:

```sh
sudo python3 scripts/advanced-bench.py --rounds 3 \
  --cases native_header_34 native_full_34 native_rewrite_exact_34 native_rewrite_prefix_34 native_rewrite_regex_34 \
  --output-dir test-results/native-rewrite-final
```

All three rewrite cases forward to `/bytes/34`. nginx uses a static URI or prefix
replacement in proxy_pass for exact/prefix cases, and native rewrite with a PCRE
capture for the regex case. Direct-backend controls call `/bytes/34` directly;
without the rewrite, the original proxy paths would return a different body size.
Redirect status/Location/empty-body behavior is verified functionally, outside
this throughput comparison, since nginx's default redirect response includes HTML.
Latest measured rewrite results and regression investigation:
[rewrite-benchmark.md](../docs/rewrite-benchmark.md).

For image-to-image regression investigations, tag a known baseline before changing
the source and alternate it with the new image:

```sh
sudo python3 scripts/compare-header-images.py --before prost:sni-control \
  --after prost:native --strip-rewrite --output-dir test-results/header-control
```

`prost:sni-control` must already identify the intended baseline image. The default
is 3 alternating rounds with 100,000 requests per header case per round. The
optional strip flag replaces rewrite-only fixture plugins with empty header
filters for older binaries, preserving route count and using identical config
for both images. The report flags throughput loss above 5% or p99 increase above
10% for investigation; image IDs identify binaries independently of workspace
source. It restores the fixture and stops its benchmark project afterward. Do
not run another build or benchmark concurrently.
This comparison also accepts `--pin-cpus`, requiring 6 distinct guest cores for
the client, proxy and single backend. Do not compare absolute latency between
pinned and unpinned runs; compare the before/after images within the same run.

When a compatible runtime image is already available, Cargo release binaries can
also be rebuilt using Linux Docker caches:

```sh
sudo python3 scripts/build-release-benchmark.py --base-image prost:native --tag prost:native
```

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

HTTP limit regression and same-image policy overhead (WSL, after preparing advanced fixtures):

```sh
sudo python3 scripts/prepare-limits-bench.py
sudo python3 scripts/compare-header-images.py --before prost:limits-control --after prost:native \
  --rounds 3 --duration 10 --pin-cpus --cases native_full_34 h2_64k tls_h1_64k h2_upload_1m grpc_stream \
  --output-dir test-results/http-limits-default-timed
sudo python3 scripts/compare-header-images.py --before prost:native --after prost:native \
  --after-config test-results/advanced-fixtures/prost-limits.toml --rounds 3 --duration 10 --pin-cpus \
  --cases native_full_34 h2_64k h2_upload_1m grpc_stream --output-dir test-results/http-limits-policy-timed
sudo python3 scripts/profile-memory.py --image prost:native --http-limits --output-dir test-results/http-limits-memory
```

`prost:limits-control` must tag the binary from before HTTP limit implementation.
Reports capture image IDs and the config hash for each label. Timed runs count
completed responses and report deadline tail streams separately; fixed request
runs remain useful for correctness and can show uneven per-connection tails.

For CPU stacks, run separately from throughput/memory measurements:

```sh
sudo python3 scripts/profile-http-cpu.py --pin-cpus --case h2_64k \
  --images prost:limits-control prost:native --output-dir test-results/http-limits-cpu
```

This requires Linux `perf` with permission to sample the Docker host processes.
The load output during profiling is diagnostic, not an uninstrumented benchmark.
If the WSL perf build cannot resolve ELF symbols, recover leaf function names
from the exact image IDs with `python3 scripts/symbolize-http-cpu.py test-results/http-limits-cpu`.
This does not recover full DWARF/inline call stacks.

Native access-log correctness/resource checks run locally with
`sudo python3 scripts/access-log-smoke.py --image prost:native`. This confines
real ENOSPC to a private 64 KiB log tmpfs while serving 10000 HTTP/2 requests.
For same-image off/on policy comparisons, `compare-header-images.py` accepts
`--before-config off.toml --after-config on.toml --capture-access-log`;
the optional capture flushes outside the timed interval and saves cumulative
writer counters, including drops. It does not fsync. Full logging and 1/16
sampling costs, default-path regression and image IDs:
[access-log-benchmark.md](../docs/access-log-benchmark.md).

Before interpreting a small A/B difference, calibrate with the **same image and
config under both labels**. `compare-header-images.py` saves every p99/CPU round,
worker runnable wait time (`schedstat`), cgroup throttling and host CPU pressure.
An A/A result is marked inapplicable to a code-change gate; absolute median
differences over 5% throughput or 10% p99 flag environmental noise. Use a fresh
output directory for each run; existing measurements are never overwritten.

```sh
sudo python3 scripts/compare-header-images.py --before prost:native --after prost:native \
  --cases h2_64k --rounds 5 --duration 12 --pin-cpus \
  --output-dir test-results/h2-aa-new
sudo python3 scripts/compare-header-images.py --before prost:native --after prost:native \
  --cases h2_64k --rounds 3 --duration 12 --pin-cpus --rps-per-client 100 \
  --output-dir test-results/h2-aa-paced-new
```

The second run offers 100 requests/second per connection: 3,200 total for the
32-connection H2 fixture. This diagnoses latency below saturation; it does not
replace the original saturated capacity test or clear a failing historical gate.
Guest CPU affinity is not an exclusive reservation on the Windows host.
Evidence and limits: [h2-validation.md](../docs/h2-validation.md).

Start a separate background soak **after all builds/benchmarks finish**:

```sh
sudo python3 scripts/start-soak.py --image prost:native \
  --cert-dir /absolute/path/to/test-certs --edge-policies \
  --output-dir test-results/soak-edge-new
```

The launcher pins proxy and backend image IDs, checks certificate lifetime for
the campaign plus an hour, records PID/command/expected completion in
`launcher.json`, and writes `run.log`/`progress.json`. Inspect `summary.json` for
the completed result; successful launch is not a passed soak. SIGTERM stops the
recorded process and preserves logs; an interrupted campaign is marked failed.
The edge variant enables Host ACL, H1 idle timeout, TCP dial retry and connection
window, probes Host denial after each hot apply, and checks idle reuse past the
header budget. Healthy traffic does not exercise retry recovery or rate-limit
rejection; their integration tests cover those paths.

[nginx proxy module](https://nginx.org/en/docs/http/ngx_http_proxy_module.html),
[Apache mod_proxy](https://httpd.apache.org/docs/2.4/mod/mod_proxy.html),
[Apache event MPM](https://httpd.apache.org/docs/2.4/mod/event.html).
