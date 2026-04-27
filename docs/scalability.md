# Scalability

## Capacity Assumptions

Capacity planning should start from:
- Target node count up to thousands.
- Key cardinality and value-size distributions.
- Read/write ratio and peak burst factors.
- Desired replication factor and consistency settings.
- Burst scenarios with massive joins and massive leaves.
- Frequent VM reboot waves and unstable network episodes.

## Horizontal Scaling Mechanics

- The ring model MUST support node additions/removals without global stop-the-world coordination.
- Data movement SHOULD be proportional to ownership deltas from consistent hashing.
- Rebalancing MUST preserve deterministic ordering metadata.
- Scaling logic MUST remain safe under concurrent join/leave/reboot events.

## Hotspot and Rebalancing Strategy

- Hot partitions SHOULD be detected through per-partition traffic metrics.
- Mitigation MAY include virtual nodes, adaptive client routing, or partition-aware throttling.
- Rebalancing operations MUST be observable and bounded.

## Large-cluster Failure Domains

- Replica placement SHOULD avoid placing all replicas in one failure domain.
- Zone/rack awareness SHOULD be configurable.
- Network partitions MUST degrade safely: availability may reduce, ordering/convergence guarantees must not.
- Repeated transient failures (reboots, flap, partial reachability) MUST be treated as first-class operational conditions.

## Performance Guardrails

- Define SLOs for p50/p95/p99 latency and write durability acknowledgment.
- Define upper bounds for acceptable replication lag.
- Define maximum tolerated hinted handoff backlog duration.
- Define guardrails for membership churn rate.
- Define guardrails for churn bursts (mass join/leave in short windows).
- Define limits for acceptable convergence delay during unstable network periods.

## Scalability Exit Criteria

- Controlled tests demonstrate stable behavior under target load.
- Ring changes and failure recovery remain within operational SLO boundaries.
- Deterministic convergence remains true under scale and fault scenarios.
