# Mail-edge validation — 2026-11-15

Local Baseless static image:
`sha256:ca677808d4ac9b288e5e4b3903470a36144a59e0dd79e51410f6d46618f65c59`.
The published CI image has a separate digest; use that exact digest for VM soak.

Local checks passed:

- Workspace tests with native defaults and with all features; 106 proxy
  integration cases in each run. The crash-helper is intentionally invoked by
  its parent test rather than independently.
- Rustfmt and Clippy across all targets/features with warnings denied.
- Six Python benchmark/parser contract tests and seven-port profile validation.
- Semgrep: zero findings and zero parse errors. Trivy: zero vulnerabilities,
  dependency metadata recognized for all four native executables (including
  the certificate agent). Baseless publication still requires its separate gate.
- Kernel inspection in `mail-runtime-smoke.py`: UID 10001, effective/bounding
  capabilities zero, NoNewPrivs enabled, root mount read-only, low container
  ports 25/993, durable TLS refresh and established session preservation.

The final 180-second protocol campaign completed with 210 SMTP sessions,
70 held IMAP checks, 35 HTTPS H1 checks and 120 H2 requests, with zero errors.
It observed three CA-issued certificates (initial issuance and two renewals),
restored account/certificate state after an agent restart, and kept existing
SMTP and STARTTLS/IMAPS IDLE sessions alive. No worker was forced to drain.
An earlier successful campaign omitted the agent-restart case; two initial
harness attempts failed due to a schema spelling and WSL bind-mount ownership.
Those failed attempts are retained as failures, not included as passing runs.

Raw local artifacts:

- `test-results/mail-edge-local-final/{summary.json,samples.jsonl,*.log}`
- `test-results/mail-runtime-final/summary.json`
- `test-results/mail-edge-{semgrep-final,trivy}.json`
- `test-results/mail-interop-final-verified/summary.json`: 10,000 requests,
  ten explicit hot-mode updates, zero errors; independent Go PROXY-v2/mTLS
  backend, real OpenSSL rejection of TLS1.0/1.1/CBC/DHE and unknown/missing SNI.

The compatibility fixture initially inherited the newer rolling default and
incorrectly required H1 sockets to survive a normal config drain. Its config now
explicitly selects hot mode to match the fixture's purpose; production defaults
and rolling drain behavior are unchanged. That failed fixture run is retained
separately and is not counted as a passing result.

Reports are ignored by Git. Disposable private keys are never committed.
The harness creates runtime state on a Linux filesystem, not a Windows-backed
WSL mount, to exercise actual ownership, locks, rename and fsync semantics.

The CA is pinned official Pebble; HTTP-01 is actually validated through the edge,
without validation bypass. Clients verify CA/hostname for SMTP/IMAP/H1. H2load
checks ALPN/HTTP2 traffic separately. The backend has an independent strict
PROXY-v2 decoder and implements SMTP/IMAP STARTTLS and IDLE commands; it is not
a message store or delivery service. Application interoperability, real public
CA deployment and a completed 24-hour campaign remain separate acceptance work.
