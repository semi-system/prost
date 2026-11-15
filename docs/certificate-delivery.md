# Certificate readiness, backend delivery and DNS-01

`prost-certd` keeps bootstrap liveness separate from certificate readiness.
`prost-certd check` probes `/healthz`: it permits the edge to start and serve
HTTP-01 while the first order is pending. `prost-certd check --certificates`
probes `/readyz`: every group must have a trusted issued certificate published
to the edge, enough remaining validity, and no pending backend acknowledgments.
The default validity margin is 60 seconds (`readiness_min_validity_seconds`).
Readiness does not replace a client's CA/hostname verification.

Private `/certificates` returns expiry, bootstrap/publication state and pending
delivery counts, never key material. `/metrics` includes
`prost_acme_certificate_not_after_seconds`, `prost_acme_certificate_bootstrap`,
`prost_acme_delivery_pending`, and `prost_acme_last_publication_seconds`, labelled
by group. Alert before expiry, on prolonged bootstrap, and on pending delivery.
These endpoints remain private; only the challenge prefix is routed publicly.

## Backend acknowledgment

Mount the certificate output read-only at the backend, retaining the existing
owner/group restrictions. Backends must resolve `current` once and load both
files from the same immutable directory. Add targets under each certificate:

```toml
delivery_retry_seconds = 10 # global, before [[certificates]]

[[certificates]]
id = "edge"
domains = ["mail.example.test"]
listeners = ["smtps", "imaps"]

[[certificates.delivery]]
id = "smtp"
address = "mail-backend:1025"
protocol = "smtp_starttls" # tls, smtp_starttls, imap_starttls
send_proxy_v2 = true       # LOCAL, zero payload, no TLVs
notify_url = "https://mail-backend:8777/reload" # optional
bearer_file = "/run/secrets/reload-token"      # optional, mode 0600
[certificates.delivery.tls]
server_name = "mail.example.test"
ca_certificate = "/run/trust/mail-ca.pem" # omit for public WebPKI
[certificates.delivery.notify_tls]
server_name = "mail-backend"
ca_certificate = "/run/trust/private-api-ca.pem"
```

The agent persists a private outbox before contacting a backend. A target is
acknowledged only after CA/hostname verification and an exact SHA-256 match of
the backend's leaf certificate against the committed leaf. HTTPS notification
success is not an acknowledgment. Notifications carry JSON
`{"version":1,"certificate":"edge","target":"smtp","fingerprint_sha256":"..."}`
and an `Idempotency-Key`; the adapter must tolerate repeated delivery. The agent
probes again on the next bounded retry. Per-target ACKs survive agent restart;
a new leaf resets them. Change a target ID when changing its delivery identity.

Targets may omit `notify_url` when a service reads the files per handshake or
the host sends SIGHUP. The supplied systemd path/service retries signals until
certificate readiness succeeds; configure delivery targets for that check to
actually verify backend adoption. No Docker socket is mounted in the agent.
Delivery does not provide atomic edge/backend deployment: the edge can already
serve the new certificate while a backend is unavailable. Alert on pending ACKs.
An ACK records successful adoption; it is not continuous backend health checking.

Up to four certificate cycles and four deliveries per cycle run concurrently.
An unavailable certificate group does not serially block every other group.
Each order, HTTP hook, TLS probe, protocol line and reply body has a finite budget.

## DNS-01 and wildcard certificates

HTTP-01 remains the default. DNS-01 uses an authenticated HTTPS adapter so the
edge needs no DNS-provider-specific credentials or SDK. The adapter owns the
provider integration and must add/remove **one TXT value**, preserving other
values during simultaneous apex/wildcard authorizations.

```toml
[dns01]
present_url = "https://dns-adapter.example.test/present"
cleanup_url = "https://dns-adapter.example.test/cleanup"
bearer_file = "/run/secrets/dns-token"
propagation_timeout_seconds = 120
# nameservers = ["192.0.2.53:53"] # optional, otherwise system resolv.conf
# [dns01.tls]                  # optional private CA / client identity
# server_name = "dns-adapter.example.test"
# ca_certificate = "/run/trust/adapter-ca.pem"

[[certificates]]
id = "tenants"
domains = ["example.test", "*.example.test"]
listeners = ["https"]
challenge = "dns01"
```

Each hook receives `version=1`, `action=present|cleanup` and
`record={name="_acme-challenge.example.test",value="..."}` as JSON, with a
Bearer token and idempotency key when configured. Return 2xx after the operation
has been durably accepted. The agent independently queries TXT visibility before
asking the CA to validate. Restrict adapter authorization to the managed zones.
There is no filesystem/network access from a traffic plugin for this operation.

Pending TXT cleanup is persisted before presentation, retried after crashes,
and runs independently of backend certificate acknowledgment. An unavailable
cleanup adapter can leave stale challenge values until it recovers; monitor
agent errors. DNS propagation through every public resolver is not guaranteed
by the local observation; CA validation remains authoritative.

Wildcard domains require DNS-01. Listener SNI allow-lists still apply separately:
add an explicit `*.example.test` entry where tenant SNI should be accepted. A
wildcard matches one label; it does not permit missing SNI or unrelated names.
Use a new certificate group ID when changing the group's domain set.

## Local verification and heavy soak

```sh
docker build -t prost:mail-stress-backend benchmarks/mail-stress
sudo python3 scripts/mail-edge-soak.py --image YOUR_IMAGE \
  --backend-image prost:mail-stress-backend --dns01 --duration 400 \
  --clients 2 --output-dir test-results/mail-dns01
sudo python3 scripts/mail-edge-soak.py --image YOUR_IMAGE \
  --backend-image prost:mail-stress-backend --duration 86400 --validity 3600 \
  --clients 8 --stress --stress-connections 128 --stress-streams 8 \
  --output-dir test-results/mail-heavy-24h
```

The fixture caches its STARTTLS certificate and reloads only through the private
HTTPS hook. A fault returns HTTP 200 without loading the new certificate: the
test requires readiness to stay false, restarts the agent with pending delivery,
then verifies recovery. A separate IMAPS IDLE connection stays application-silent
for over five minutes. Neither fixture stores or delivers actual mail.

Stress rounds ramp connections, then cycle small requests, 64 KiB/1 MiB download,
1 MiB upload and 1 MiB streaming over TLS/HTTP2. `stress-rounds.jsonl` reports
completed/status/error counts, p99 at 0.1 ms resolution, proxy/backend/agent CPU,
working set, FDs, tasks, throttling and driver CPU/RSS. Requests still in flight
at a timed round's cutoff are recorded separately. Profile IP caps are raised
only for the synthetic admin workload; policy rejection tests run before load.
The load generator shares the host: CPU, backend or network saturation limits
interpretation. A short run is not a completed 24-hour soak or an nginx comparison.

The mail Compose profile sets an explicit 65,536 descriptor limit for the edge.
Size that OS limit alongside frontend/request/PROXY budgets. PROXY-v2 pools stay
isolated per frontend identity, retain up to min(configured idle pool, 32) sockets
per backend for five seconds, and share the process-wide PROXY socket semaphore.
This permits HTTP/2-to-HTTP/1 fanout reuse without a burst of TIME_WAIT sockets.
Local address/descriptor exhaustion is reported as a local failure, not evidence
that an otherwise healthy backend should be passively ejected.
