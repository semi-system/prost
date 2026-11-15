# Mail interoperability evidence

The neutral regression suite includes strict binary PROXY v2 parsing, verified
backend mTLS, SMTP STARTTLS, IMAP/POP3 health probes, SNI policy and TCP half-close.
The independent Go backend in `benchmarks/mail` verifies PROXY v2 precedes the
TLS ClientHello. Run `scripts/mail-interop-smoke.py` for its 10,000-request fixture.
That paced correctness/resource fixture is not an nginx capacity comparison.

The newer seven-port ACME/SMTP/IMAP campaign is reproducible with
`scripts/mail-edge-soak.py`; configuration and scope are documented in
[mail-edge.md](mail-edge.md). Raw reports are local artifacts under `test-results`
and are not committed, including any disposable test keys. Use a new output
directory for each campaign and record the exact image digest. Reports distinguish
failed, interrupted, running and completed campaigns. Do not combine evidence
from different binaries or treat a protocol fixture as a production mail service.
