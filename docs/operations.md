# Operations

This document is the CLI reference and operational runbook for Tourillon. For
the step-by-step lifecycle sequences (startup, join, restart, drain) see the
`docs/lifecycle/` documents. For certificate management and the PKI model see
`docs/security.md`.

---

## CLI Reference

Tourillon ships a single `tourillon` entry point. Install shell autocompletion
once per shell with:

```bash
tourillon --install-completion
```

---

### PKI Bootstrap

Before provisioning any node, generate the CA certificate and private key. The
CA key must be kept offline and must never be placed on cluster nodes.

```bash
tourillon pki ca \
  --common-name "Tourillon CA" \
  --out-dir ./pki \
  --days 3650
```

Writes `./pki/ca.crt` and `./pki/ca.key` (mode 0600). Aborts if either file
already exists; pass `--force` to overwrite.

---

### Config Generation

**Generate a fully self-contained node config**

```bash
tourillon config generate \
  --node-id        node-1 \
  --ca-cert        ./pki/ca.crt \
  --ca-key         ./pki/ca.key \
  --san-dns        node-1.internal \
  --san-ip         10.0.0.1 \
  --kv-bind        0.0.0.0:7000 \
  --kv-advertise   node-1.internal:7000 \
  --peer-bind      0.0.0.0:7001 \
  --peer-advertise node-1.internal:7001 \
  --out            ./node-1.toml
```

Issues a server certificate signed by the supplied CA, embeds the certificate,
private key, and CA certificate as inline base64 in the output file, and writes
the file at mode 0600. The `peer` endpoint serves both inter-node traffic
(replication, gossip, hinted handoff) and operator `tourctl` connections.

**Generate a client context**

```bash
tourillon config generate-context prod \
  --ca-cert       ./pki/ca.crt \
  --ca-key        ./pki/ca.key \
  --kv-endpoint   node-1.internal:7000 \
  --peer-endpoint node-1.internal:7001
```

Issues a client certificate and writes a `prod` entry in
`~/.config/tourillon/contexts.toml`. At least one of `--kv-endpoint` or
`--peer-endpoint` must be supplied.

**Config file format (reference)**

```toml
[node]
id        = "node-1"              # unique node identifier within the cluster
data_dir  = "/var/lib/tourillon"  # directory for durable log and state files
log_level = "info"                # one of: debug, info, warning, error

[tls]
cert_data = "LS0t..."  # base64 PEM — server cert
key_data  = "LS0t..."  # base64 PEM — private key
ca_data   = "LS0t..."  # base64 PEM — CA cert

[servers.kv]
bind      = "0.0.0.0:7000"
advertise = "node-1.internal:7000"

[servers.peer]
bind      = "0.0.0.0:7001"
advertise = "node-1.internal:7001"

[cluster]
seeds              = ["node-2.internal:7001", "node-3.internal:7001"]
replication_factor = 3
```

> **Security note.** `config.toml` embeds the node's private key. Write with
> mode 0600, never commit to version control, and never allow world-readable
> permissions.

---

### Starting a Node

```bash
tourillon node start --config ./node-1.toml
```

Validates TLS material and config, then binds two mTLS listeners. A Rich
status panel is displayed. Press Ctrl-C or send SIGTERM for a clean shutdown.

**Exit codes**

| Code | Meaning |
|------|---------|
| 0    | Success or clean shutdown |
| 1    | User or configuration error |
| 2    | Internal or unexpected error |

---

### `tourctl` Contexts

`tourctl` stores named cluster connections in
`~/.config/tourillon/contexts.toml`. The active context drives all commands
that do not supply explicit connection flags.

```bash
tourctl config list            # list all contexts; mark the active one
tourctl config use-context prod  # set prod as the active context
```

**Contexts file format (reference)**

```toml
current-context = "prod"

[[contexts]]
name = "prod"

  [contexts.cluster]
  name    = "prod-cluster"
  ca_data = "LS0t..."

  [contexts.endpoints]
  kv   = "node-1.internal:7000"
  peer = "node-1.internal:7001"

  [contexts.credentials]
  cert_data = "LS0t..."
  key_data  = "LS0t..."
```

All writes to `contexts.toml` are atomic (`os.replace` via a temporary file)
so a crash mid-write never corrupts the active context list.

---

## Continuous Churn and Unstable Network Assumptions

Operational procedures must remain safe under:

- Massive node joins at any time.
- Massive node leaves or simultaneous crashes.
- Frequent VM reboots and rolling infrastructure restarts.
- Latency spikes, packet loss, and intermittent network partitions concurrently
  with active rebalance or replication traffic.

For the specific behaviour of the system under each of these conditions see
`docs/rebalance.md — Worst-Case Scenarios`.

---

## Observability and Alerting

Minimum telemetry includes:

- Request latency and error rates by operation kind on both listeners.
- Replication lag and hinted handoff queue depth.
- Ring change events (epoch advances, join/leave notices) and membership churn
  rate.
- TLS handshake failure counts and certificate validation error counts.

Alert on: sustained replication lag above the p99 target in `docs/scalability.md`,
hinted handoff queue depth growing faster than it drains, certificate expiry within
30 days, burst membership churn exceeding the configured maximum epochs per minute,
and partition flapping (the same node oscillating between phases repeatedly).

---

## Incident Runbooks

### Node isolation and recovery

**Detection:** a node stops responding to peer probes; its local reachability
transitions to `DEAD` on one or more peers; hinted handoff queues for that
node grow.

**Immediate mitigation:** confirm the node process is running (`systemctl status
tourillon`). If the process is alive but unresponsive, inspect the asyncio task
queue via the `tourctl log tail` command for stuck coroutines.

**Recovery:** if the node restarts cleanly its `READY` phase will re-announce
itself and hinted handoffs will drain automatically. If the node must be
removed permanently, issue `tourctl node leave` while the process is still
reachable, or — if the process is dead — remove the node from the ring via
`tourctl ring remove-dead` (Milestone 4).

### Certificate rollover failure

**Detection:** TLS handshake failure log entries on `servers.kv` or
`servers.peer`; client error rates spike.

**Immediate mitigation:** verify that `cert_data` in `config.toml` has not
expired (`openssl x509 -noout -dates -in <(base64 -d <<< "$CERT_DATA")`).
If expired, re-issue the certificate with `tourillon config generate` and
restart the affected node.

**Recovery:** rolling restart after deploying new configs. Until Milestone 4
there is no zero-downtime rotation procedure.

### Ring instability and rebalance storm

**Detection:** ring epoch counter advancing faster than one per second over a
sustained window; `tourctl ring inspect` shows dozens of in-flight transfers.

**Immediate mitigation:** pause new joins (`tourctl node hold-joins`). Reduce
the rebalance rate-limit fraction in `TourillonConfig` and apply via rolling
config reload. Identify the source of the churn (mass join, mass leave, or
partition flap).

**Recovery:** once the storm subsides and the epoch counter stabilises, resume
normal operations. Verify convergence with `tourctl ring inspect` and confirm
all partitions show committed ownership at the current epoch.

### High error-rate event

**Detection:** `TEMPORARY_UNAVAILABLE` or `INTERNAL_ERROR` rates exceed 1%
of requests over 1 minute.

**Immediate mitigation:** identify the affected partition range via structured
logs. If a specific node is the source, check its gossip phase; a `DRAINING`
node that is slow to complete its drain will cause write fallback overhead.

**Recovery:** if the error is transient (network hiccup), it resolves once
replication catches up. If a node is stuck in `DRAINING`, check the rebalance
log for blocked transfers and unblock the target or source.

---

## Backup, Restore, and Upgrade Policies

- **Backup scope:** data volume plus the local log segment containing ordering
  metadata is required for deterministic replay. Backing up data without
  metadata produces a restore that cannot safely accept new writes.
- **Restore validation:** after restore, verify the ordering watermark of every
  restored partition matches the backup manifest before allowing writes.
- **Rolling upgrades:** follow the rolling upgrade runbook (Milestone 4).
  Each node should be upgraded one at a time with a verification step between
  each restart.
- **Irreversible upgrade steps:** any step that advances the protocol version
  in a way that older nodes cannot parse must be documented with an explicit
  rollback procedure before the step is taken.
