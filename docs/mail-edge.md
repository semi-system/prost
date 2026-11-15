# Generic mail edge

The seven-port profile in [deploy/mail](../deploy/mail/compose.yaml) provides:

| Public port | Edge behavior | Private backend |
|---|---|---|
| 25 / 587 | Byte-transparent TCP, including application STARTTLS | SMTP / submission |
| 143 | Byte-transparent TCP, including application STARTTLS and IDLE | IMAP |
| 465 / 993 | Strict-SNI TLS termination | Plaintext SMTP / IMAP |
| 443 | HTTPS with HTTP/2 and HTTP/1.1 ALPN | HTTP admin |
| 80 | HTTP-01 challenge forwarding; otherwise HTTPS 308 | Certificate agent |

Change all example DNS names, backend addresses and admin CIDRs before use.
The documentation CIDRs deliberately deny real public clients. The admin Host
allow-list returns 421; the required IP allow-list returns 403. Empty allow-lists
and IPv4/IPv6 /0 entries fail validation. No CSP/HSTS filter is installed: the
application supplies its own policy. HTTP header deadlines are 10 seconds.

All application upstreams use binary PROXY v2, preserving actual peer IP/ports,
without TLVs. Health checks send the 16-byte LOCAL command with zero payload;
mail probes use TCP and admin probes use HTTP. Never expose those backend ports
to untrusted clients. Trusted ingress PROXY protocol is opt-in separately.

SMTP idle is 300 seconds. IMAP `idle_timeout_ms=0` disables the inactivity cutoff;
write, handshake and half-close budgets remain bounded. Admission has per-IP
connection caps and SMTP connection-rate limiting. Backend DNS uses bounded TTL
and negative caching; it resolves again after expiry without a config update.

TLS defaults allow only TLS 1.2/1.3 and ECDHE/AEAD suites. The profile disables
session resumption/tickets and rejects missing or unknown SNI. Certificate renewal
preserves each listener's SNI allow-list even when a SAN cert covers mail and admin.
Upstream TLS and client certificate authentication are independent options; see
[examples/mail-edge.toml](../examples/mail-edge.toml) for a PROXY-before-mTLS profile
and the protocol-aware SMTP STARTTLS/IMAP health check options.

## Certificates and deployment

Read [acme.md](acme.md), edit both TOML files in deploy/mail, then run:

```sh
export PROST_IMAGE=ghcr.io/semi-system/prost@sha256:YOUR_VERIFIED_DIGEST
docker compose -f deploy/mail/compose.yaml up -d
```

The image runs as UID/GID 10001, with a read-only root filesystem, all capabilities
dropped and no-new-privileges. High container ports map to standard host ports;
no NET_BIND_SERVICE capability is required. Only state/socket volumes are writable
by the edge; certificate output is read-only there. Containers log to stdout with
bounded Docker log rotation. The admin Unix socket is never published over TCP.
Attach private backend containers to the `prost-mail` network.

The seed is used on first startup only. Later config edits must be committed via
`prostctl apply`; certificate renewal is automatic through `tls.reload_many`.
For a new domain, provision its certificate group and SNI policy deliberately.
The certificate agent does not watch S3 objects or local config files for changes.

## Verification

`scripts/mail-edge-soak.py` creates a fully isolated edge, certificate agent,
strict PROXY decoder, SMTP/IMAP protocol fixture and Pebble CA. HTTP-01 validation
is real: validation bypass is never enabled. It verifies issuance and renewal,
STARTTLS through plaintext listeners, implicit TLS listeners, held IMAP IDLE,
source IP, LOCAL probes, Host/CIDR denials, redirects, SNI and HTTP/2 traffic.

```sh
sudo python3 scripts/mail-edge-soak.py --image "$PROST_IMAGE" --duration 240
sudo python3 scripts/mail-edge-soak.py --image "$PROST_IMAGE" --duration 86400 \
  --validity 3600 --output-dir test-results/mail-24h
```

This fixture does not authenticate users, store messages or deliver mail. A
production qualification must also use the deployed mail implementation and
real DNS/CA policy. A newly started 24-hour campaign is not a completed soak.
