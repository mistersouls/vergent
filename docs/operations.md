# Operations

## CLI Reference

Tourillon ships a single `tourillon` entry point that exposes all operational
commands. Install autocompletion once per shell with:

```bash
tourillon --install-completion
```

### PKI Bootstrap

Before starting any node, generate the certificate hierarchy with the `pki`
sub-commands. The CA private key must be kept offline and must never be copied
to cluster nodes.

**Generate the CA**

```bash
tourillon pki ca \
  --common-name "Tourillon CA" \
  --out-dir ./pki \
  --days 3650
```

Writes `./pki/ca.crt` and `./pki/ca.key` (mode 0600). Aborts if either file
already exists; pass `--force` to overwrite.

**Issue a server certificate**

```bash
tourillon pki server \
  --ca-cert ./pki/ca.crt \
  --ca-key  ./pki/ca.key \
  --common-name "node-1.internal" \
  --san-dns node-1.internal \
  --san-ip  10.0.0.1 \
  --out-dir ./pki/node-1 \
  --days 365
```

At least one `--san-dns` or `--san-ip` is required; a bare CN-only certificate
is rejected because modern TLS clients ignore the CN field.

**Issue a client certificate**

```bash
tourillon pki client \
  --ca-cert ./pki/ca.crt \
  --ca-key  ./pki/ca.key \
  --common-name "client-app" \
  --out-dir ./pki/clients/app \
  --days 365
```

### Starting a Node

```bash
tourillon node start \
  --node-id  node-1 \
  --host     0.0.0.0 \
  --port     7000 \
  --certfile ./pki/node-1/server.crt \
  --keyfile  ./pki/node-1/server.key \
  --cafile   ./pki/ca.crt
```

The command validates all certificate paths before entering the asyncio loop.
A Rich status panel is displayed while the node is running. Press Ctrl-C or
send SIGTERM to shut down cleanly; the process exits with code 0.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0    | Success or clean shutdown |
| 1    | User or configuration error |
| 2    | Internal or unexpected error |

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
