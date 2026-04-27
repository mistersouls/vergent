# Architecture

## System Context and Goals

Tourillon targets predictable behavior in large, failure-prone environments.
The architecture combines leaderless request handling with deterministic per-key update
evolution so any node can safely serve traffic behind standard load balancers.

## Node Responsibilities

Each node MUST:
- Serve read and write requests for keys it owns or proxies.
- Participate in membership and ring-state dissemination.
- Replicate updates to responsible peers.
- Persist local state and update metadata needed for deterministic replay.

Each node SHOULD:
- Expose health and readiness signals for load balancers.
- Apply bounded backpressure under overload.

## Data Partitioning and Replication Ring

- The cluster MUST map keys to partitions through a consistent-hashing ring.
- Each partition MUST define a deterministic replica set ordering.
- Replication factor `N` MUST be configurable and validated at startup.
- Ring transitions (join/leave/failure) MUST preserve deterministic ownership calculations for a given ring version.
- Any ownership change MUST produce a deterministic rebalance plan for affected partitions.

Rationale: deterministic ownership prevents split-brain interpretation of who is responsible for each key.

## Rebalance Model

- Rebalance MUST be triggered on ring membership changes and topology updates.
- Rebalance MUST move only partitions impacted by ownership deltas.
- Source and destination nodes MUST preserve per-key ordering metadata during transfer.
- Rebalance SHOULD be rate-limited to avoid saturating foreground read/write traffic.
- Rebalance progress MUST be observable (queued partitions, transferred bytes, lag).
- Rebalance completion MUST be validated before retiring previous owners.

Rationale: bounded and observable rebalancing preserves availability while keeping deterministic convergence guarantees.

## Read/Write Flows

Write flow (logical):
1. Client sends request to any node.
2. Receiving node computes ring owner set.
3. Node assigns deterministic update metadata for the key.
4. Node replicates according to durability policy.
5. Node returns ack based on configured write consistency.

Read flow (logical):
1. Client sends read to any node.
2. Receiving node resolves owner set.
3. Node fetches local/remote candidate states.
4. Node returns value selected by deterministic ordering rules.

## Failure and Recovery Model

- Temporary peer failure MUST trigger hinted handoff buffering.
- Recovered peers MUST receive missed updates in deterministic order.
- Recovery replay MUST be idempotent.
- Node restart MUST rebuild in-memory indexes from durable log/state before serving writes.
- Recovery-driven ownership changes MUST reuse the same deterministic rebalance rules.

## Architectural Invariants

- No central leader or single coordinator is required for normal operation.
- For a fixed ring version and key history, all healthy replicas MUST derive the same visible state.
- Transport security is mandatory and described in `docs/security.md`.
- Message contract and compatibility are defined in `docs/protocol.md`.
