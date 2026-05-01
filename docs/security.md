# Security

This document defines Tourillon's security model: the threat surface, the
mandatory mTLS requirements, the certificate lifecycle, and the security
invariants that must hold at every node lifecycle phase. It is normative for
any component that touches TLS contexts, certificate material, or connection
acceptance.

---

## 1. Threat Model

Tourillon assumes it will be deployed on hostile networks and must defend
against the following primary classes of threat:

- **Passive traffic interception.** An attacker positioned on the network path
  between two nodes (or between a client and a node) reads data in transit.
  Mitigation: TLS encryption on all connections.
- **Active man-in-the-middle attacks.** An attacker intercepts and modifies
  messages between two parties, or impersonates a node. Mitigation: mutual TLS
  with certificate chain validation. Both parties must present valid
  certificates signed by the trusted CA before any application data is
  exchanged.
- **Unauthorised node enrollment.** A malicious or misconfigured process claims
  to be a legitimate cluster node. Mitigation: every node certificate must be
  signed by the cluster CA. An unsigned or self-signed certificate is rejected
  at the TLS handshake layer.
- **Replay of valid messages.** An attacker records a legitimate envelope and
  replays it later. Mitigation: envelope correlation IDs and HLC ordering
  metadata make replayed messages either identifiable as duplicates (same
  correlation ID) or correctly handled by the idempotent apply path (same HLC
  produces no state change).
- **Compromised client certificates.** A revoked or expired client certificate
  is used to connect. Mitigation: certificate expiry is enforced by the TLS
  layer; revocation strategy is defined in §4.

---

## 2. Mandatory mTLS

All communications in Tourillon — client-to-node, node-to-node, and
operator-to-node via `tourctl` — MUST use mutual TLS. There is no plaintext
fallback. There is no mode that relaxes mutual authentication. A connection
attempt that does not present a valid client certificate chain is rejected
during the TLS handshake before any Tourillon framing byte is processed.

The enforcement point is the `ssl.SSLContext` configured with
`ssl.CERT_REQUIRED` on each listener. Creating a context without this flag is
a bug and violates this invariant. Code review must flag any `ssl.SSLContext`
that does not set `CERT_REQUIRED`.

---

## 3. Certificate Requirements at Each Lifecycle Phase

Security constraints are not uniform across lifecycle phases. The following
table maps each phase to the certificate operations that MUST have succeeded
before the phase can be entered.

| Phase | Certificate precondition |
|-------|--------------------------|
| Before `IDLE` | Node server cert, private key, and CA cert must be loaded and validated at startup. An expired or mismatched cert aborts startup before any socket is bound. |
| `IDLE → JOINING` | The node's cert must cover the `servers.peer` advertise address in its Subject Alternative Names, because seed nodes will validate the certificate against the address they are connecting to. |
| `JOINING → READY` | No additional certificate check; the certs that passed the join phase handshake remain valid. |
| `READY` | On restart: cert validity is re-checked during the startup sequence. A cert that expiry during runtime does not immediately close existing connections, but new connections from peers will fail until the cert is rotated. |
| `DRAINING` | Cert must remain valid for the duration of the drain. A cert that expires mid-drain causes all new inbound `rebalance.transfer` connections to be rejected. |

---

## 4. PKI Bootstrap and Certificate Lifecycle

### 4.1 Generating the Certificate Authority

The CA private key is the root of trust for the entire cluster. It must be
generated once, stored offline, and never placed on any cluster node.

```bash
tourillon pki ca --common-name "Tourillon CA" --out-dir ./pki --days 3650
```

This produces `./pki/ca.crt` and `./pki/ca.key`, both written with mode 0600.
The command aborts if either file already exists; pass `--force` to overwrite.

### 4.2 Issuing Node Certificates

Each cluster node requires a server certificate signed by the CA. The
certificate must include at least one Subject Alternative Name (`--san-dns` or
`--san-ip`). A certificate with only a Common Name and no SAN is rejected
because modern TLS clients do not use the CN for hostname verification.

Node certificates are embedded inline in the TOML config file as base64-encoded
PEM material. They are never stored as separate files on the node. The
`tourillon config generate` command issues the certificate and writes the
complete config in a single step.

### 4.3 Issuing Client Certificates

Operator clients (`tourctl`) and application clients connecting to `servers.kv`
require client certificates signed by the cluster CA. Client certs are issued
by `tourillon config generate-context` and embedded inline in the `contexts.toml`
file.

### 4.4 Private Key Handling

- All private key material is written with file mode 0600.
- Private key bytes are never emitted to log output at any verbosity level.
- The CLI refuses to overwrite an existing key file unless `--force` is passed.
- In-memory, private key material is held as bytes and never converted to a
  string that could appear in a traceback or `repr()` output.

### 4.5 Certificate Rotation

Certificate rotation (replacing a valid certificate with a new one before or
after expiry) without cluster downtime is planned for Milestone 4 via a
dual-certificate window: the node temporarily trusts both the old and new CA
so that the cluster can make a rolling transition. Until that milestone,
operators must coordinate a rolling restart to deploy new certificates.

Trust roots must be explicitly configured. No automatic certificate trust
expansion is performed.

### 4.6 Revocation Strategy

The revocation strategy for this version is **short-lived certificates with
manual coordination**. Certificate expiry enforced by the TLS layer acts as
an implicit revocation mechanism: a certificate that expires more than a few
days after issuance presents a limited revocation attack surface in the
absence of a CRL or OCSP infrastructure. A dedicated revocation facility
(CRL or OCSP stapling) is deferred to Milestone 4.

---

## 5. Dual-Endpoint Trust Anchors

Each node exposes two endpoints (`servers.kv` and `servers.peer`) with
independent `ssl.SSLContext` instances. This independence allows:

- The KV plane to trust a client CA authoritative for application clients.
- The peer plane to trust a separate peer/operator CA, restricted to cluster
  nodes and `tourctl` operators.

Both listeners use the same server certificate and private key (the node's
identity). The separation is in the **trust anchor for verifying inbound
client certificates**, not in the node's own identity material.

An operator wishing to enforce strict separation between application clients
and operator clients configures a different `ca_data` in `[servers.kv]` and
`[servers.peer]`. If no per-section `ca_data` is specified, both listeners
fall back to the `[tls].ca_data` value.

---

## 6. Security Hardening Baseline

- TLS version minimum: TLS 1.2; TLS 1.3 preferred where available.
- Cipher suites: defer to Python's `ssl.SSLContext.set_ciphers` secure defaults
  for the TLS version in use; no manual cipher string should override this
  without a documented security rationale.
- Private keys must be protected at rest; the filesystem permission (0600) is
  the enforced minimum.
- Security-relevant events (TLS handshake failure, certificate validation
  rejection, unknown envelope kind) must be logged at WARNING level with a
  correlation ID so that incidents can be reconstructed from logs.

---

## 7. Security Acceptance Criteria

- A connection without a valid client certificate is rejected at the TLS
  handshake layer; no application-level byte is returned.
- A connection with an expired server certificate is rejected by the connecting
  client; the node logs a certificate validation failure.
- Invalid or self-signed certificates are rejected deterministically on both
  sides of every connection.
- Private key material does not appear in logs, tracebacks, or `repr()` output.
- `util.write_config_file` is the only code path allowed to write config or
  context files; it enforces mode 0600 on every write.
