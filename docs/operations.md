# Operations

## CLI Reference

Tourillon ships a single `tourillon` entry point that exposes all operational
commands. Install autocompletion once per shell with:

```bash
tourillon --install-completion
```

### PKI Bootstrap

Before starting any node, generate the CA certificate and private key with the
`pki ca` sub-command. The CA private key must be kept offline and must never be
copied to cluster nodes.

**Generate the CA**

```bash
tourillon pki ca \
  --common-name "Tourillon CA" \
  --out-dir ./pki \
  --days 3650
```

Writes `./pki/ca.crt` and `./pki/ca.key` (mode 0600). Aborts if either file
already exists; pass `--force` to overwrite.


### Configuration File

Every node reads its runtime parameters from a TOML config file. CLI flags
always take precedence over the file, so you can ship one config per node and
still override individual values on the command line.

**Why TOML.** `tomllib` is part of the Python 3.11+ standard library; no
additional dependency is needed to read the config file. TOML's strict native
typing (integers, booleans, arrays are always typed) eliminates implicit
coercions that YAML introduces (booleans `yes`/`no`, the Norway problem, octal
literals). Writing TOML for `config generate` uses `tomli-w`, which is the only
additional runtime dependency the config system adds. See
`docs/architecture.md` — *Config file format and location* for the full
rationale.

**Generate a fully self-contained node config**

```bash
tourillon config generate \
  --node-id       node-1 \
  --ca-cert       ./pki/ca.crt \
  --ca-key        ./pki/ca.key \
  --san-dns       node-1.internal \
  --san-ip        10.0.0.1 \
  --kv-bind       0.0.0.0:7000 \
  --kv-advertise  node-1.internal:7000 \
  --peer-bind     0.0.0.0:7001 \
  --peer-advertise node-1.internal:7001 \
  --out           ./node-1.toml
```

This command issues a server certificate signed by the supplied CA,
base64-encodes the certificate, private key, and CA certificate, and writes a
complete `config.toml` with all TLS material embedded inline. No separate PKI
step is required after `tourillon pki ca`. The output file is written at mode
`0600` because it contains private key material; it must never be readable by
other users.

Each endpoint is configured with a **bind** address (the interface and port the
OS socket listens on) and an optional **advertise** address (the host and port
gossiped to peers and returned to clients for routing). These differ when the
node runs behind NAT, a load balancer, or a container port mapping. When
`--kv-advertise` or `--peer-advertise` is omitted the bind value is used for
both. The `peer` endpoint serves both inter-node traffic (replication, gossip,
hinted handoff) and operator client connections from `tourctl`.

**Generate a fully self-contained client context**

```bash
tourillon config generate-context prod \
  --ca-cert       ./pki/ca.crt \
  --ca-key        ./pki/ca.key \
  --kv-endpoint   node-1.internal:7000 \
  --peer-endpoint node-1.internal:7001
```

This command issues a client certificate signed by the supplied CA,
base64-encodes the certificate, private key, and CA certificate, and writes (or
updates) a `prod` entry in `~/.config/tourillon/contexts.toml` with all TLS
material embedded inline. `--kv-endpoint` and `--peer-endpoint` are both
optional but at least one must be supplied. A context with only `--kv-endpoint`
is sufficient for `tourctl kv` operations; a context with only
`--peer-endpoint` is sufficient for `tourctl ring inspect`, `tourctl log tail`,
and other operator commands. An optional `--common-name` flag sets the
certificate CN (defaults to the context name). An optional `--days` flag
controls certificate validity. Because the private key is embedded, `tourillon`
enforces mode `0600` on `contexts.toml` on every write via an atomic
`os.replace` so a crash mid-write never corrupts the existing context list.

**Config file format**

```toml
[node]
id        = "node-1"              # unique node identifier within the cluster
data_dir  = "/var/lib/tourillon"  # directory for durable log and state files
log_level = "info"                # one of: debug, info, warning, error

[tls]
cert_data = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t..."  # base64 PEM — server cert
key_data  = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVkt..."  # base64 PEM — private key
ca_data   = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t..."  # base64 PEM — CA cert

[servers.kv]
bind      = "0.0.0.0:7000"          # interface and port the KV listener binds to
advertise = "node-1.internal:7000"  # address advertised to clients and peers
# ca_data can be overridden here to trust a different CA for KV clients

[servers.peer]
bind      = "0.0.0.0:7001"          # interface and port the peer listener binds to
advertise = "node-1.internal:7001"  # address advertised to peers and operator clients (tourctl)
# ca_data can be overridden here to trust a separate peer/operator CA

[cluster]
seeds = [                 # addresses of seed nodes for initial ring discovery (Milestone 2+)
  "node-2.internal:7000",
  "node-3.internal:7000",
]
replication_factor = 3    # number of replicas per partition (Milestone 2+)
```

> **Security note.** `config.toml` is written at mode `0600` because it embeds
> the node's private key. Treat it with the same care as a private key file.
> Never check it into version control or allow world-readable permissions.

The `[tls]` section supplies the node's identity (server cert + key) and the
default CA for peer verification. Both `[servers.kv]` and `[servers.peer]`
inherit the `[tls]` `ca_data` unless they override it with their own `ca_data`
key, which allows separate mTLS trust anchors for the data plane and the peer/
operator plane.

**Specifying a custom config path**

```bash
tourillon node start --config ./node-1.toml
```

The `--config` flag is accepted by any `tourillon` command that reads the
config file. The default location when `--config` is omitted is
`~/.config/tourillon/config.toml`.

### Starting a Node

```bash
tourillon node start --config ./node-1.toml
```

The command validates all TLS material and config values before entering the
asyncio loop. It then binds two separate TCP listeners — one on `servers.kv`
for client KV traffic and one on `servers.peer` for inter-node and operator
traffic — each with its own mTLS SSL context. A Rich status panel is displayed while the node
is running. Press Ctrl-C or send SIGTERM to shut down cleanly; the process exits
with code 0.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0    | Success or clean shutdown |
| 1    | User or configuration error |
| 2    | Internal or unexpected error |

### tourctl Contexts

`tourctl` stores named cluster connections in
`~/.config/tourillon/contexts.toml`. A context bundles a cluster definition
(seed node addresses and their CA certificate) with a credentials definition
(client cert and key used when connecting). The active context drives all
`tourctl` commands that do not supply explicit connection flags.

Context entries are created and updated exclusively by
`tourillon config generate-context` (see *Configuration File* above). `tourctl`
itself never issues certificates or writes PKI material; it only reads and
navigates the contexts file.

**List available contexts**

```bash
tourctl config list
```

Prints all context names and marks the currently active one.

**Switch the active context**

```bash
tourctl config use-context prod
```

After this, `tourctl kv get`, `tourctl ring inspect`, and every other `tourctl`
sub-command will use the `prod` context without requiring any additional flags.

**Contexts file format (reference)**

```toml
current-context = "prod"

[[contexts]]
name = "prod"

  [contexts.cluster]
  name    = "prod-cluster"
  ca_data = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t..."

  [contexts.endpoints]
  kv   = "node-1.internal:7000"  # optional — required for kv operations
  peer = "node-1.internal:7001"  # optional — required for admin/operator operations

  [contexts.credentials]
  cert_data = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0t..."
  key_data  = "LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVkt..."
```

The contexts file is written only by `tourillon config generate-context`. No
running `tourillon` node process ever modifies it, and `tourctl` never writes
PKI material to it. All writes go through a single utility that atomically
replaces the file (`os.replace` via a temp file) so a crash mid-write never
corrupts the existing context list.

Supported topologies SHOULD include:
- Single-region multi-node cluster.
- Multi-zone cluster behind standard load balancers.
- Incremental scale-out with rolling node additions.

All topologies MUST preserve leaderless request routing semantics.

## Continuous Churn and Unstable Network Assumptions

- Operations MUST assume that massive node joins can happen at any time.
- Operations MUST assume that massive node leaves can happen at any time.
- Operations MUST assume frequent VM reboots and rolling infrastructure restarts.
- Operations MUST assume unstable network conditions (latency spikes, packet loss, intermittent partitions).
- Control-plane and data-plane procedures MUST remain safe under concurrent churn and network instability.

## Bootstrap, Join, Leave Procedures

Bootstrap:
- Initial ring configuration MUST define replication factor and seed members.
- Cluster MUST refuse startup if ring configuration is invalid.

Join:
- New nodes MUST complete identity validation and ring synchronization before serving writes.
- Rebalance actions SHOULD be rate-limited to protect live traffic.
- Bulk joins MUST be admitted in controlled batches with backpressure.

Leave/failure:
- Graceful leave SHOULD transfer ownership cleanly.
- Ungraceful failure MUST trigger replication and hint workflows.
- Simultaneous multi-node leave/failure events MUST trigger degraded-but-safe mode with deterministic recovery ordering.
- VM reboot storms MUST be handled as repeated temporary failures without violating convergence guarantees.

## Observability and Alerting

Minimum telemetry MUST include:
- Request latency and error rates by operation.
- Replication lag and hinted handoff queue depth.
- Ring change events and membership churn.
- TLS handshake failures and certificate validation errors.

Alerts SHOULD cover sustained replication lag, large handoff backlog, and certificate expiry risk.
Alerts SHOULD also cover burst membership churn, repeated node reboot patterns, and partition flapping.

## Incident Runbooks

Runbooks MUST exist for:
- Node isolation and recovery.
- Certificate rollover failure.
- Ring instability and rebalance storms.
- High error-rate events.
- Massive join events.
- Massive leave/failure events.
- VM reboot storms.
- Intermittent network degradation and partition flapping.

Each runbook SHOULD include detection signals, immediate mitigation, and verification steps.

## Backup/Restore and Upgrade Policies

- Backup strategy MUST define scope (data + metadata required for deterministic replay).
- Restore procedure MUST validate ordering metadata integrity.
- Upgrades SHOULD be rolling and version-compatible per `docs/protocol.md`.
- Any irreversible upgrade step MUST have explicit rollback guidance.
