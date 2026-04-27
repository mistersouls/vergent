# Convergence

## Deterministic Update Ordering

Tourillon convergence depends on a strict per-key ordering model.

- Every update MUST carry ordering metadata sufficient to totally order updates for one key.
- Ordering metadata MUST be stable across retries and replication.
- Ties MUST be resolved deterministically.

A reference order can be defined as `(logical_time, origin_node_id, sequence)`.

## Conflict Model and Resolution

- State evolution for a key MUST be append-only.
- Each append-only item MUST be a version tuple based on `HLC` metadata plus the value payload.
- Concurrent client writes MUST produce multiple valid versions for the same key.
- `GET` MUST return all visible concurrent versions, not a single winner.
- Delete operations MUST be represented as ordered tombstone versions in the same append-only stream.

Rationale: append-only version history preserves causality and prevents lossy conflict collapse under concurrency.

## Replica State Evolution Rules

- Replica apply logic MUST be monotonic with respect to deterministic order.
- Applying the same update multiple times MUST not change final state after first application.
- Out-of-order arrival MUST be corrected by buffering or deferred apply.

## Hinted Handoff Ordering Guarantees

- Hinted updates MUST preserve original per-key ordering metadata.
- Delivery from handoff queues MUST respect deterministic order for each key.
- Expired or invalid hints MUST be observable via metrics and logs.

## Convergence Proof Sketch and Assumptions

Assumptions:
- Ring membership eventually stabilizes for enough time to exchange updates.
- Network partitions heal eventually.
- Durable metadata is not corrupted.

Sketch:
- Since all updates for a key share deterministic ordering metadata and replicas apply monotonic rules, replay order differences do not affect final state.
- Idempotent apply and deterministic tie-breakers eliminate non-deterministic divergence.

Residual risk:
- Long-lived partitions may delay convergence but must not produce contradictory final states once healed.


