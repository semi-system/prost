# Automatic ACME certificates

HTTP-01 is the default. [Certificate delivery and DNS-01](certificate-delivery.md)
documents wildcard issuance, durable backend ACK/retry, readiness and expiry metrics.

`prost-certd` is a separate native process in the same Baseless image. It owns
the ACME account and private key store, serves HTTP-01 tokens on a private
address, and commits validated TLS updates through the edge's Unix socket.
It never shares keys with the admin web application or stores them in a DB.
No file watcher, S3 polling or manual `prostctl apply` is needed for renewal.

Start with [deploy/mail/certd.toml](../deploy/mail/certd.toml) and the matching
[edge profile](../deploy/mail/edge.toml). Configure public DNS and inbound port 80
for every exact DNS identifier. HTTP-01 does not issue wildcard certificates.
The edge routes only the challenge prefix to the agent; `/healthz` and `/metrics`
on the agent's private port are not exposed by that route.

The agent creates a self-signed bootstrap certificate before becoming healthy,
so the edge can bind TLS and route challenges during first issuance. External
clients must not trust that bootstrap certificate. Issuance failure leaves the
last committed cert active; on first deployment no publicly trusted certificate
is available until the CA order succeeds. Monitor issuance errors and expiry.
Do not use example domains or publish this unconfigured sample as a live service.

Issuance uses HTTPS, bounded ACME response sizes, order deadlines and retry
backoff. `directory_ca` optionally supplies a private CA for an ACME server
such as Pebble. Without it, verification uses public WebPKI roots. ACME terms
acceptance is explicit. Account state is persisted mode 0600 with an exclusive
process lock. Certificate/key pairs are validated before storage, written into
immutable version directories, fsynced, and selected by an atomic `current`
symlink. Each configured group keeps its current version and seven predecessors.
The agent state and certificate volumes must reside on a local filesystem with
working locks, atomic rename and fsync semantics.

On expiry minus `renew_before_seconds`, the agent issues a replacement. A batch
`tls.reload_many` transaction validates all target listeners, persists managed
config/artifacts and publishes one generation under the same coordinator as
other config writes. Generation conflicts are retried with fresh config export
(at most four attempts); other failures back off. Established sessions retain
their snapshot. Renewal does not start a rolling drain or replace workers.
Ordinary config updates still use the normal rolling behavior.

The agent retains existing listener-specific SNI allow-lists. Setting
`add_sni_names=true` explicitly adds every group domain to every target listener;
leave it false when mail and admin use different permitted names.
`default_certificate=false` updates only matching named certificates.
For a changed set of domains, use a new group ID; do not silently reuse old
account metadata. Restart the agent after editing its static config. Restarting
reuses account credentials and republishes an existing issued certificate before
ordering another when needed.

The runtime API response can be lost after commit; a retry may create a further
identical TLS generation. It cannot partially update the listeners. Existing
streams are unaffected. Check generation, certificate fingerprints and metrics
if the API reports uncertain directory fsync durability.

## Delivering certificates to mail backends

Mount `prost-mail-certificates` read-only into services that need their own
STARTTLS keys. Resolve `/certificates/edge/current/fullchain.pem` and
`privkey.pem` together; during concurrent rotation, pin the resolved version
directory before reading both files. Read permissions default to owner 10001
only (0600 files / 0700 directories). Use a dedicated shared group with
`shared_certificate_group=true` for 0640/0750 access if necessary. Keep that group
out of the web application's container.

Services may load files on each handshake. For services requiring a reload
signal, the agent atomically writes `publication.json` after a successful edge
commit. The host can watch that event with systemd.path and signal only the
configured backend containers. Install/adapt the three
`deploy/mail/prost-cert-notify.*` files: copy the script to
`/usr/local/sbin/prost-cert-notify`, the units to `/etc/systemd/system`, and
replace container names and Docker data-root path. Enable the `.path` unit.
Run the service once manually to synchronize an already-issued certificate.
The backend must actually implement SIGHUP reload; configure fingerprint delivery
targets and `--agent-container` in the notification script. The service retries
until `prost-certd check --certificates` succeeds. Inspect its logs and alert
on unit failures. No Docker socket is mounted inside the edge or agent.

The certificate symlink is prepared before the edge transaction; the publication
marker is emitted after commit. A backend reading files on demand can observe
the new, validated cert before the edge does. Backend notifications are event
delivery, not a distributed atomic transaction. The optional private delivery
outbox retries missing verified backend ACKs after a host/agent restart; it does
not roll back edge TLS. Without configured targets, replay remains an operator duty.

## Metrics and tests

Private agent `/metrics` exports `prost_acme_issued_total`,
`prost_acme_failures_total` and `prost_acme_publications_total`. Edge Prometheus
adds `prost_tls_certificate_not_after_seconds` and
`prost_tls_certificate_seconds_remaining` per listener/certificate, alongside
existing handshake, traffic, admission and generation metrics. Configure a
loopback metrics address and collect it through a local trusted collector.

`scripts/mail-edge-soak.py` uses the pinned official Pebble image, custom trusted
directory CA and actual HTTP-01 validation through port 80's route. Short-lived
certificates force renewal while SMTP STARTTLS and IMAP IDLE traffic is active.
The test CA is never used by the production Compose profile. Short smoke runs
and CI do not replace a full 24-hour campaign or application acceptance tests.
