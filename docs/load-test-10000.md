# MVP load test — 2026-11-07

Measured the running `prost:mvp` debug image in Docker Engine inside WSL2 Ubuntu.
Host: Intel Core i7-10700, 16 logical CPUs; WSL memory limit approximately 7.72 GiB.
The load generator, proxy and backend share this machine.

## Workload and method

- 10,000 measured GET requests per run, 100 concurrent workers, HTTP/1.1 keep-alive.
- Each worker sends one request at a time; initial connection time is included.
- 200 warmup requests before each measurement, excluded from measured counts.
- Backend: Python demo `green`; JSON response body 34 bytes.
- Plaintext HTTP, native connection-limit plugin enabled; no TLS or WASM.
- No retries. Every success requires HTTP 200 and the expected JSON backend/path.
- Read Linux cgroup v2 CPU counters before/after load; sample memory and FDs every
  50 ms. Memory peaks are sampled, so short spikes between samples may be missed.
- RAM below means cgroup working set (`memory.current - inactive_file`), not
  exclusively Rust heap or process RSS. CPU 100% means one logical core.

## Results

| Measurement | Run 1 | Run 2 |
|---|---:|---:|
| Successful requests | 10,000 / 10,000 | 10,000 / 10,000 |
| Failed requests | 0 | 0 |
| Duration | 6.135 s | 5.423 s |
| Throughput | 1,630 req/s | 1,844 req/s |
| Mean latency | 47.77 ms | 47.69 ms |
| p50 | 47.64 ms | 47.82 ms |
| p95 | 60.11 ms | 60.24 ms |
| p99 | 84.06 ms | 77.76 ms |
| Maximum latency | 1,674.83 ms | 832.29 ms |
| Prost CPU time | 7.886 core-seconds | 7.836 core-seconds |
| Prost average CPU | 128.51% (~1.29 cores) | 144.45% (~1.44 cores) |
| Prost CPU per request | 788.62 µs | 783.56 µs |
| Prost working set after warmup | 9.76 MiB | 22.03 MiB |
| Prost sampled peak working set | 27.45 MiB | 27.39 MiB |
| Prost working set 2 s after load | 25.45 MiB | 25.49 MiB |
| Prost sampled peak main-process FDs | 350 | 312 |
| Prost main-process FDs 2 s after load | 30 | 30 |
| Backend sampled peak working set | 18.57 MiB | 18.61 MiB |
| Backend average CPU | 68.20% | 77.76% |

The repeated run had a similar memory peak and CPU time per request. Runtime
status afterward reported zero active connections and one live generation.
These short runs do not establish absence of memory leaks. Allocator caches,
connection pools and temporary blocking-worker threads can retain resources
after load; the report preserves thread/task counts and anonymous-memory samples.

## Direct-backend control

An additional 10,000 requests were sent directly from WSL to the backend container
IP at the same concurrency. Result: 9,984 successes, 16 connection/read timeouts
with a 10-second request deadline; successful-request p50 was 44.14 ms. The demo
backend therefore has latency and connection-admission limitations of its own.
Do not interpret this run as evidence that the proxy is faster than direct I/O,
or attribute the proxy run's full ~48 ms mean latency to Prost.

This measures the current MVP and demo backend, not production capacity. It does
not cover TLS, WASM, large payloads, sustained traffic or separate load-generator
hardware. Release-build and faster-backend measurements remain future work.

## Reproduce

From WSL, in the project directory, with the Compose demo running on `green`:

```sh
sudo python3 scripts/loadtest.py \
  --requests 10000 --concurrency 100 \
  --container prost-prost-1 --container prost-green-1 \
  --output test-results/proxy-10000-c100.json
```

If the demo is on `blue`, first apply `/examples/green.toml`, or use
`--backend blue --container prost-blue-1` instead.

Raw locally generated reports (ignored by Git):

- `test-results/proxy-10000-c100.json`
- `test-results/proxy-10000-c100-repeat.json`
- `test-results/backend-direct-10000-c100.json`

Each JSON includes image IDs, request/status/error counts, latency distribution,
CPU deltas, baseline/peak/post-load memory and timestamped cgroup samples.
