# Operations

## Deployment Topologies

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
