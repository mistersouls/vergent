# Architecture

## System Context and Goals

Tourillon targets predictable behavior in large, failure-prone environments.
The architecture combines leaderless request handling with deterministic per-key update
evolution so any node can safely serve traffic behind standard load balancers.

## Keyspace Model

A keyspace is a client-supplied logical scope that partitions the key namespace,
analogous to namespaces in Kubernetes or keyspaces in Cassandra. Every operation
in Tourillon is expressed against a specific keyspace: there is no "default" or
implicit keyspace. Callers provide the keyspace alongside the key for every read,
write, and delete.

Keyspaces serve two roles. At the application level they provide logical isolation
between datasets so that keys with identical names in different keyspaces never
collide. At the infrastructure level the keyspace participates, together with the
key, in computing the consistent-hash token that determines ring ownership and
replica placement.

### StoreKey — the canonical addressing unit

The pair `(keyspace, key)` is represented as a single immutable value object named
`StoreKey`. Every storage operation, every `Version`, every `Tombstone`, and every
log entry carries a `StoreKey` as its address. Passing a `StoreKey` rather than two
loose string arguments provides a stable, typed unit of identity that can be passed
across layer boundaries without caller confusion about argument ordering.

## Storage Command Objects

The `LocalStoragePort` methods each accept a dedicated command object rather than
a flat list of parameters. The three operations are `WriteOp`, `ReadOp`, and
`DeleteOp`. Each carries a `StoreKey` address plus the arguments specific to
the operation: `value` and `now_ms` for a write, `now_ms` only for a deletion, and
the address alone for a read.

This design means that extending an operation — for example by adding a consistency
level, a TTL, or a partition identifier — requires adding a field to the relevant
command dataclass rather than changing the method signature. Callers are decoupled
from future additions by construction.

## Partition Reference

A partition token identifies one slice of the consistent-hash ring. Within the
storage layer the partition is a routing and grouping concern, not a value property.
`Version` and `Tombstone` do not carry a partition token because the partition is
a function of the `StoreKey` and the current ring state. It can always be recomputed
and must not be persisted as part of the causal record.

The router layer resolves `StoreKey → partition token → replica set` before
calling the storage port. The storage implementation may organise its internal
structures by partition for rebalance efficiency, but this organisation is opaque
to callers of `LocalStoragePort`.

### Adding partition_id in a future milestone

When the ring layer is introduced (Milestone 2), the router will have already
resolved the partition token before calling the storage port. At that point,
`WriteOp` and `DeleteOp` can each gain an optional `partition_id` field
that the router pre-fills. The storage implementation can use that field to
organise data by partition — which makes partition transfer during rebalance
cheaper — without any change to the `LocalStoragePort` method signatures.
`ReadOp` does not need a `partition_id` field because reads follow the same
`StoreKey`-based lookup path regardless of which partition owns the key.

This evolution path is safe precisely because of the command object design: adding
a new field to a frozen dataclass is backwards-compatible for all existing callers
that construct the object, and the storage port method signatures are unchanged.

## Node Responsibilities

Each node MUST:
- Serve read and write requests for keys it owns or proxies.
- Participate in membership and ring-state dissemination.
- Replicate updates to responsible peers.
- Persist local state and update metadata needed for deterministic replay.

Each node SHOULD:
- Expose health and readiness signals for load balancers.
- Apply bounded backpressure under overload.

## Transport Layer

Tourillon uses a custom binary transport built on asyncio streams with mandatory
mutual TLS. The transport is introduced alongside the single-node storage core so
that end-to-end connectivity can be validated before replication logic is added.

Each accepted connection is handed to a `ConnectionHandler`, an async callable that
receives two injected coroutines: `receive()`, which suspends until the next `Envelope`
arrives from the peer, and `send(envelope)`, which writes a framed `Envelope` to the
wire. The handler owns the application logic for the lifetime of that connection and
never touches framing, serialisation, or TLS directly.

A `Dispatcher` routes incoming envelopes to the handler registered for their
`kind`. Handlers are registered at startup and are never modified at runtime.
The transport layer closes the connection immediately for any frame whose `kind`
has no registered handler, without returning an application-level response.

The server component wraps `asyncio.start_server` with an SSL context that enforces
mutual TLS. There is no cleartext fallback. Connections that fail certificate
validation are closed immediately without returning any application-level error.

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
1. Client sends a `WriteOp(StoreKey, value, now_ms)` to any node over mTLS.
2. Receiving node resolves `StoreKey → partition token → replica set` via the ring.
3. Node ticks the HLC clock to obtain deterministic ordering metadata.
4. Node records the resulting `Version` in its local log and replicates to peers.
5. Node returns an ack based on the configured write consistency.

Read flow (logical):
1. Client sends a `ReadOp(StoreKey)` to any node over mTLS.
2. Receiving node resolves the owner set for the `StoreKey`.
3. Node fetches local and remote candidate states.
4. Node returns the value selected by the deterministic HLC ordering rule.

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
