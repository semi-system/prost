# Architecture and MVP boundaries

`prost-config` contains serializable types. `prost-transport`, `prost-http` and
`prost-plugin` are reusable independently of the daemon. `prost-core` owns
snapshots and listeners. `prost-control` implements Unix-socket framing and
administration; `prost-cli` provides the two executables. All runtime code targets
Linux; Windows development is through Docker inside WSL.

## Configuration transactions

One coordinator serializes validate/apply/endpoint patches. Writes require the
expected generation. Preparation validates artifacts, constructs TLS configs and
compiles WASM on the blocking pool. New listener sockets are bound before commit.
An atomic file rename is the commit point. Files are fsynced before rename; the
parent directory is fsynced afterward. The new `Arc<Snapshot>` is published using
ArcSwap. No config polling, file watching, process replacement or signal reload is
involved. Health check timers probe backends; they never inspect config files.

If the parent-directory fsync fails **after** rename, the commit is already
visible on disk. The daemon publishes it and returns `durable: false` rather than
pretending the old configuration is still authoritative. The operator should fix
the storage issue before relying on restart durability. Pre-rename errors leave
the running snapshot untouched. These are local POSIX-filesystem semantics; NFS
and Windows bind-mounted config directories have not been qualified.

The managed config and lock file must live in a writable directory. Artifacts
(certificates, keys, CA bundles, WASM) are copied by SHA-256 into the managed
artifact directory; saved config points there. Validation may stage unreferenced
artifacts, but does not publish config. Explicit `artifacts.gc` serializes with
preparation and protects the current config and live generations. The store has
one exclusive daemon owner; see [artifact-gc.md](artifact-gc.md).

Request cancellation cannot interrupt a started apply transaction. After a lost
response, inspect `status` and compare its generation/hash before retrying.
Generation is persisted inside the config. Endpoint enable/disable updates the
same durable config, not an independent volatile overlay.

Existing TCP sessions and WebSocket tunnels retain their resources. HTTP reads
the latest snapshot at the start of each request/HTTP2 stream. Response bodies
retain the snapshot until completion or cancellation. Listener identity and
protocol on an occupied address cannot change in place. Runtime settings require
restart. TCP and HTTP listeners are separate; this version does not multiplex
arbitrary protocols on the same port.

## HTTP and plugins

Hyper forwards bodies incrementally with bounded protocol buffers. Upstream
clients reuse pools when transport/client settings and endpoint identity are compatible. gRPC uses
explicit `http2 = true` on the upstream, including h2c. WebSocket supports the
HTTP/1.1 upgrade handshake. CONNECT and RFC8441 extended CONNECT are outside MVP.
The configured upstream idle timeout also bounds waiting for response headers;
raise it for applications with long uploads before headers are returned.

Route ordering is priority, longest prefix, then first declaration. If no route
matches, the listener-level upstream is used; without one the result is 404.
Native request filters run before that 404, so a redirect-only listener can return
without an upstream. The rewrite filter changes the forwarded path/query while
retaining the original route, method and streaming body. Redirects finish the
request phase and run local native response filters. See [rewrite.md](rewrite.md).
Upstreams support weighted round-robin, least active request/session load and
bounded consistent-hash rings. Compatible least-load counters survive updates;
HTTP leases follow the existing body/tunnel lifetime guard. Client pools survive
algorithm/weight/administrative changes when transport and endpoint identity are
unchanged. See [load-balancing.md](load-balancing.md).
Backup tiers and optional endpoint `max_active` admission share the same lifetime
leases. Tracked state survives compatible algorithm/cap changes; overload rejects
immediately without queuing or marking health down. See [upstream-capacity.md](upstream-capacity.md).
Health checks verify TCP/TLS connectivity or perform HTTP GET checks with expected
status, timeout and fall/rise thresholds; see [advanced-features.md](advanced-features.md). Passive
transport errors exclude an endpoint until an active check succeeds by default.
An optional passive policy adds bounded rolling failure windows, cooldown and
configured upstream HTTP statuses; recovery requires a successful probe after
cooldown. Compatible health observations are copied between generations; changing
only passive policy resets its window while keeping active health observations.
See [passive-health.md](passive-health.md). Requests are never automatically retried.

TLS listeners select certificates using exact SNI names or single-label wildcards.
Certificate/key bundles are prepared and staged as artifacts before commit. A
policy can reject absent/unmatched SNI; the default retains the original fallback
certificate behavior. TLS state is reused when unchanged, while rotations create
new state for newly accepted connections; existing sessions retain their state.
Details and runnable smoke: [tls-sni.md](tls-sni.md).

Native plugins implement `prost_plugin::NativePlugin` and register before start.
Their callbacks must return quickly and must not block Tokio workers. The
built-in `connection_limit` uses connection-lifetime semaphore permits and belongs
on listeners. Put WASM blocks on listeners or routes; use `name = "wasm"` and
`module = "/absolute/path/to/module.wasm"`. WAT modules are accepted for examples.

WASM ABI v1 exports `memory`, `prost_abi_version() -> i32`,
`alloc(length: i32) -> i32`, `handle(pointer: i32, length: i32) -> i64`.
Input is JSON Event; output is JSON Action, returned as `(pointer << 32) | length`.
An instance is disposable after every hook, so the guest needs no deallocator.
Event hooks are `connection`, `request_headers`, `upstream`, `response_headers`,
and `end`. `end` runs when a response/tunnel finishes or is cancelled, and after
TCP relay completion. It is best-effort during forced shutdown. Actions can deny,
select an existing upstream, or set HTTP headers. Header values exposed to WASM
are text and repeated fields are collapsed; this is a deliberately small MVP API.
There is no body-transform API or WASI. `examples/add-header.wat` is a tiny fixture,
not a general-purpose guest SDK. WASM instances have memory, fuel and concurrency
limits; module compilation and native plugins are trusted administrative inputs.

## Reference: Broom

Reviewed `semi-system/broom` at commit
`d4ad73166b5ddfe64ac28cf78ba54685c4472c99`:

- [iSCSI daemon](https://github.com/semi-system/broom/blob/d4ad73166b5ddfe64ac28cf78ba54685c4472c99/mgmt/src/iscsid/mod.rs):
  Unix control socket, semaphore admission, session-owned Arcs and TCP liveness.
- [Connection writer](https://github.com/semi-system/broom/blob/d4ad73166b5ddfe64ac28cf78ba54685c4472c99/mgmt/src/iscsid/conn.rs):
  bounded work and write timeout for stalled readers.
- [Build script](https://github.com/semi-system/broom/blob/d4ad73166b5ddfe64ac28cf78ba54685c4472c99/build.sh):
  Linux/WSL development and build artifacts outside Windows-mounted source.

Prost applies these design principles independently; no Broom source is copied.
Broom's application protocol is in user space but its Tokio sockets still use
the kernel TCP/IP stack. Prost does too. DPDK/AF_XDP, io_uring, splice, static musl
packaging and profile-driven optimizations are later experiments, not MVP claims.

## Deferred qualification

The MVP has integration coverage, not a production certification. Soak 24 hours,
hardware benchmarks against nginx, fuzz campaigns, dependency/security review,
per-instance plugin histograms, private pool queue/hit-miss statistics, admin groups and power-loss/storage-controller qualification
remain later work. Process-crash, injected fsync and confined tmpfs ENOSPC tests
are described in [artifact-gc.md](artifact-gc.md). Metrics currently expose active
connections, HTTP requests, traffic errors, stream bytes, commits and time to
response headers.
Detailed current endpoint, passive health, admission, downstream TLS, transaction
and snapshot/process metrics are available; see [observability.md](observability.md).
Heap/RSS profiling and the tested TLS buffer tradeoff are documented in
[memory-profiling.md](memory-profiling.md).

HTTP request body limits and separate upload/header/response-body/socket-write
deadlines are described in [http-limits.md](http-limits.md). These preserve
streaming, apply route policy per request snapshot, and keep client body faults
out of upstream passive health. Opt-in H2 header-block deadlines and per-stream
DATA completion budgets now use public IO/Executor/body hooks, including a
partially queued final frame; see [mvp-bundle.md](mvp-bundle.md).

Native access logging uses an opt-in bounded queue and a separate buffered writer;
explicit Runtime API flush/reopen supports rotation without file watching. Log
body observation follows stream/tunnel lifetime and retains the request's original
generation. Actual HTTP upstream transport leases expose open/close/connect/TLS
metrics across pool replacement; response reuse tracking is optional. Duration
includes DNS/TCP/TLS and reuse counters are not private pool hit/miss statistics.
Configuration, failure semantics and local full-log-filesystem tests:
[access-log.md](access-log.md).
