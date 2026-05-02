# Proposal: KV Data Plane

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Draft
**Date:** 2026-05-02
**Sequence:** 006
**Schema version:** 1

---

## Summary

This proposal defines everything that happens when a client writes or reads
data: the storage model, the addressing unit, deterministic ordering via the
Hybrid Logical Clock, the `put`, `get`, and `delete` operations, convergence
semantics (idempotent apply, concurrent version resolution, tombstones), ring
routing from key to replica set, replication fan-out and quorum acknowledgement,
proxy paths for non-owner nodes, local failure detection, and hinted handoff
for temporarily unreachable replicas. The `tourctl kv get`, `tourctl kv put`,
and `tourctl kv delete` commands are the entry points that derive all of this.

---

## Motivation

A key-value store is only useful if it answers two questions consistently:
"what value does this key have right now?" and "did my write succeed?" Without
a rigorous ordering model, concurrent writes from multiple clients produce
silently divergent state. Without replication and quorum, a single node failure
loses acknowledged writes. Without hinted handoff, acknowledged writes are lost
for the duration of a node's downtime.

Tourillon's answer is: all writes are timestamped with a Hybrid Logical Clock
(HLC) that provides both causal ordering and wall-clock proximity; writes are
replicated to `rf` nodes before acknowledgement; and if a replica is temporarily
unreachable, the write is stored as a hint and delivered when the replica
recovers. No write that has been quorum-acknowledged is ever lost.

---

## CLI contract

### `tourctl kv put`

```
$ tourctl kv put --keyspace users --key alice --value '{"email":"alice@example.com"}'
✓ Put accepted (HLC: 1746185040123.0001.node-1)

# Pipe a binary value
$ cat profile.bin | tourctl kv put --keyspace profiles --key alice --stdin
✓ Put accepted (HLC: 1746185041200.0000.node-2)

# Error — node not READY
$ tourctl kv put --keyspace users --key alice --value hello
✗ Node is not ready (phase: IDLE). Use `tourctl node join` first.
  exit code 1

# Error — quorum not reached (< ⌊rf/2⌋ + 1 replicas acknowledged)
$ tourctl kv put --keyspace users --key alice --value hello
✗ Write failed: quorum not reached (1/3 replicas acknowledged).
  Retrying is safe — the write is idempotent.
  exit code 2
```

### `tourctl kv get`

```
$ tourctl kv get --keyspace users --key alice
{"email":"alice@example.com"}

# Key does not exist
$ tourctl kv get --keyspace users --key bob
✗ Key not found: users/bob
  exit code 2

# Key has been deleted (tombstone)
$ tourctl kv get --keyspace users --key charlie
✗ Key users/charlie was deleted (tombstone at HLC 1746185099.0000.node-1)
  exit code 2

# Show full version metadata
$ tourctl kv get --keyspace users --key alice --verbose
  Key:       users/alice
  Value:     {"email":"alice@example.com"}
  HLC:       1746185040123.0001.node-1
  Keyspace:  users
  Partition: 42
  Replicas:  node-1 (primary), node-2, node-3
```

### `tourctl kv delete`

```
$ tourctl kv delete --keyspace users --key alice
✓ Deleted users/alice (tombstone HLC: 1746185120.0000.node-1)

# Delete is idempotent — already deleted
$ tourctl kv delete --keyspace users --key alice
✓ Deleted users/alice (already a tombstone — no-op)
```

---

## Design

### Addressing: `StoreKey`

Every storage operation is addressed by `StoreKey(keyspace, key)`. The
keyspace provides logical namespace isolation: two keys with the same name in
different keyspaces never collide. At the infrastructure level the keyspace
participates (together with the key) in computing the consistent-hash token
that determines ring ownership.

There is no default or implicit keyspace: callers must supply the keyspace for
every operation. This catches a class of routing bugs at the API boundary rather
than at runtime.

### Deterministic ordering: Hybrid Logical Clock (HLC)

Each HLC timestamp is a triple `(wall_ms, logical, node_id)`:

- `wall_ms` — millisecond-precision UTC wall clock, advanced to `max(local, received)` on every event to ensure monotonicity across nodes.
- `logical` — a counter that increments when two events share the same `wall_ms`.
- `node_id` — the stable string identifier of the node that generated the timestamp.

Total order: compare `wall_ms` first, then `logical`, then `node_id`
lexicographically. `node_id` as a tiebreaker is deterministic and never
produces a tie. No `time.time()` calls escape the HLC; no `random` tiebreaks;
no wall-clock comparisons outside the HLC layer.

### Operation objects

```python
@dataclass(frozen=True)
class WriteOp:
    key: StoreKey
    value: bytes
    now_ms: int   # caller-supplied HLC wall component

@dataclass(frozen=True)
class ReadOp:
    key: StoreKey

@dataclass(frozen=True)
class DeleteOp:
    key: StoreKey
    now_ms: int
```

Using dedicated dataclasses rather than flat parameter lists means adding a
field (TTL, consistency level, partition hint) only requires extending the
relevant dataclass — the `LocalStoragePort` method signatures never change.

### Storage model: `Version` and `Tombstone`

A `Version` is the result of a successful write:

```
Version(key: StoreKey, value: bytes, hlc: HLC)
```

A `Tombstone` is the result of a delete:

```
Tombstone(key: StoreKey, hlc: HLC)
```

Both are immutable. Neither stores a partition token — the partition is a
function of `StoreKey` and the current ring and must not be frozen into the
causal record. Every storage event — write, replicate, or handoff — is recorded
as one of these two types.

### Convergence semantics

**Idempotent apply:** applying the same `Version` or `Tombstone` twice has no
effect. Duplicate `replicate` messages are harmless.

**Last-write-wins per HLC:** for a given `StoreKey`, the `Version` or
`Tombstone` with the highest HLC is the visible winner. If two events share an
identical HLC triple (impossible in practice due to the `node_id` component,
but handled defensively), the event with the higher `node_id` lexicographically
wins.

**Out-of-order delivery:** delivering events in any order eventually converges
to the same result. The storage layer applies each event and records it; the
visible value is recomputed as `max(all versions and tombstones for this key)`
at read time.

**Tombstone preservation:** a `Tombstone` is a first-class event, not the
absence of a `Version`. It must be replicated to all replicas and stored until
its retention window expires (future milestone). A `get` on a key that has a
tombstone with no later `Version` returns "deleted", not "not found".

### Ring routing: key → replica set

The routing path from a `StoreKey` to an ordered list of replica nodes:

```
StoreKey(keyspace, key)
    │
    ▼
HashSpace.hash(keyspace + key)           → token: int ∈ [0, 2**bits)
    │
    ▼
Partitioner.placement_for_token(token)   → PartitionPlacement (partition + owner vnode)
    │
    ▼
PlacementStrategy.preference_list(...)   → [node_id, ...]  length ≤ rf
```

The first entry is the primary replica; subsequent entries are secondary
replicas. This list is the **only** input to routing — no component below the
placement layer decides which node to contact.

Phase eligibility for the preference list:

| Phase | Eligible | Reason |
|-------|:--------:|--------|
| `IDLE` | No | No partition data; not participating. |
| `JOINING` | No | Incomplete data; excluded from client routing. |
| `READY` | Yes | Fully operational. |
| `PAUSED` | No | Everything suspended; refuses all replication and reads. |
| `DRAINING` | Yes (reads via peer channel) | Still holds all data. Returns `TEMPORARY_UNAVAILABLE` for primary writes; coordinators fall through to next eligible member. Reachable via peer listener (not KV listener, which is closed). |
| `FAILED` | No | Inert. |

### Write path

1. Client sends `kv.put` envelope to the KV TCP listener of any `READY` node
   (the KV listener is only open in `READY`).
2. Receiving node resolves `StoreKey → preference_list`.
3. Node ticks the HLC to get a fresh timestamp.
4. Node records the `Version` in its local log.
5. Node fans out a `replicate` envelope to every other member of the preference
   list concurrently.
6. Node awaits `⌊rf/2⌋ + 1` acknowledgements (quorum). If a replica is `DEAD`
   locally, a hint is stored instead (see Hinted Handoff).
7. Node returns `kv.put.ack` with the HLC to the client.

Step 4 (local record) happens before step 5 (fan-out): the coordinator is always
the first to commit its own write.

### Read path

1. Client sends `kv.get` to the KV TCP listener of any `READY` node.
2. Receiving node resolves `preference_list`.
3. Node fetches the local value (if it is in the list) and sends `replicate.read`
   to the other replicas. (Quorum read: `⌊rf/2⌋ + 1` responses required.)
4. Node returns the value with the highest HLC from all responses. If all
   responses are tombstones, returns "deleted".

### Proxy path

A `READY` node that receives a `kv.put` or `kv.get` for a key it does not own
(i.e. it is not in the preference list) proxies the request to the first
eligible node in the preference list **via the peer channel** (not the KV
channel). The proxying node never touches the payload; it forwards the raw
envelope and returns the response to the original caller. Proxying is
transparent to the client.

Inter-node communication for replication, quorum reads, and proxying always
uses `servers.peer`. The `servers.kv` socket is exclusively for client
connections (only open in `READY`).

### Local failure detection

Each node maintains a private, per-peer reachability classification:

| Class | Meaning |
|-------|---------|
| `REACHABLE` | Recent operations to this peer succeeded. |
| `SUSPECT` | One or more recent operations failed. |
| `DEAD` | Unresponsive long enough to be treated as permanently unreachable for routing. |

Reachability is driven **only by operation outcomes** — there is no heartbeat
timer. A failed `replicate`, `handoff.push`, or probe raises suspicion. A
successful operation lowers it. Reachability is **never gossiped**; it is
private to each node. Two nodes may hold different views of the same peer
simultaneously — this is correct and expected.

### Hinted handoff

When the coordinator would replicate to a `DEAD` peer, it stores a **hint**
locally instead:

```
Hint(target_node_id: str, version: Version | Tombstone)
```

The hint carries the original `Version` or `Tombstone` with its original HLC.
The coordinator's own HLC is never substituted. This preserves causal ordering
on replay.

When the target peer transitions back to `REACHABLE`, the coordinator delivers
all pending hints via the `handoff.push` envelope kind, in HLC order. The
receiving node applies them idempotently. After successful delivery the hint is
removed from the queue.

Quorum acknowledgement: a write is considered successful once `⌊rf/2⌋ + 1`
replicas confirm — live acks count, hints count as "pending". The write is not
held until the `DEAD` peer recovers; it is acknowledged to the client
immediately, and the hint ensures eventual delivery.

### Core invariants

- `StoreKey` is the canonical addressing unit at every layer boundary. No loose
  `(keyspace, key)` pairs cross layer boundaries.
- HLC is the **only** mechanism for ordering events. No wall-clock comparison
  outside the HLC layer. No `random` tiebreaks.
- `Version` and `Tombstone` never store a partition token. The partition is
  recomputed from `StoreKey` and current ring state.
- A `Tombstone` is replicated and stored like a `Version` — it is never
  discarded silently.
- Hints carry the original HLC — never the hint-queue insertion timestamp.
- The KV TCP listener is **only open** in `READY`. Client traffic is never
  accepted in any other phase.
- A `PAUSED` node refuses all KV operations including inbound `replicate`
  envelopes. Hints queued for a `PAUSED` peer are not delivered until it
  resumes.
- For `DRAINING` primary: `put`/`delete` returns `TEMPORARY_UNAVAILABLE`;
  `get` is served via the peer channel. Coordinators automatically fall through
  to the next preference-list member for writes.

---

## Design decisions

### Decision: HLC over Lamport clocks or wall-clock timestamps

**Alternatives considered:** Lamport logical clocks (no wall-clock component);
pure wall-clock (system time).
**Chosen because:** Lamport clocks cannot be sorted alongside real time, making
debugging and external consistency reasoning hard. Pure wall-clock timestamps
produce incorrect ordering when clocks are skewed across nodes. HLC combines
wall-clock proximity with causal ordering: in the common case timestamps sort
by wall time; when clocks are skewed the logical counter prevents regressions.

### Decision: last-write-wins with HLC tiebreak

**Alternatives considered:** multi-version concurrency with explicit conflict
resolution callbacks; vector clocks.
**Chosen because:** multi-version storage and caller-supplied conflict resolution
require a richer API contract that significantly complicates client code. Vector
clocks grow proportional to cluster size and require gossip to stay bounded.
LWW with HLC is simple, deterministic, and correct for the expected use cases
(configuration stores, session data, distributed counters via CAS — planned for
a future milestone).

### Decision: `StoreKey` as the addressing unit

**Alternatives considered:** passing `(keyspace: str, key: str)` as separate
arguments throughout.
**Chosen because:** two-argument calls are fragile — callers can accidentally
transpose arguments. A single typed value object eliminates the ambiguity,
provides a stable unit that can be passed across layer boundaries, and hashes
correctly as a dict key or set member.

### Decision: hinted handoff preserves original HLC

**Alternatives considered:** re-timestamp the hint at delivery time.
**Chosen because:** re-timestamping would make the hint appear as a newer event
than writes that happened while the target was down. This breaks convergence:
a write at T=10 would overwrite a write at T=15 if the hint for T=10 is
re-delivered at T=20. Preserving the original HLC ensures that replay always
converges to the same state as if no downtime occurred.

### Decision: proxy path is transparent to the client

**Alternatives considered:** return a redirect response and let the client
retry against the correct node.
**Chosen because:** redirects require clients to implement retry logic and to
understand the ring topology — which is an internal concern. Transparent proxying
keeps the client contract simple: send a request to any node, receive a
response. The trade-off is one extra network hop for proxied requests, which
is acceptable given that the ring distributes a random-access workload evenly.

---

## Interfaces (informative)

```python
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class StoreKey:
    keyspace: str
    key: str


@dataclass(frozen=True)
class HLC:
    """Hybrid Logical Clock timestamp."""
    wall_ms:  int    # milliseconds UTC
    logical:  int    # counter for same-millisecond events
    node_id:  str    # stable tiebreaker

    def __lt__(self, other: "HLC") -> bool: ...
    def __le__(self, other: "HLC") -> bool: ...


@dataclass(frozen=True)
class Version:
    key:   StoreKey
    value: bytes
    hlc:   HLC


@dataclass(frozen=True)
class Tombstone:
    key: StoreKey
    hlc: HLC


@dataclass(frozen=True)
class WriteOp:
    key:    StoreKey
    value:  bytes
    now_ms: int


@dataclass(frozen=True)
class ReadOp:
    key: StoreKey


@dataclass(frozen=True)
class DeleteOp:
    key:    StoreKey
    now_ms: int


class LocalStoragePort(Protocol):
    """Local KV storage adapter.

    The store is append-only at the version level: writing the same key twice
    produces two Version records; the visible value is always the one with the
    highest HLC. Callers never delete Version records directly; they issue a
    DeleteOp which produces a Tombstone.
    """

    async def write(self, op: WriteOp) -> Version: ...
    async def read(self, op: ReadOp) -> Version | Tombstone | None: ...
    async def delete(self, op: DeleteOp) -> Tombstone: ...
    async def apply(self, event: Version | Tombstone) -> None:
        """Idempotently apply a replicated event."""
        ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | In-memory node, `READY` | `put(keyspace="u", key="alice", value=b"v")` | Returns `Version` with HLC; `get` returns `b"v"` |
| 2 | In-memory node | Apply same `Version` twice | Key history contains it exactly once |
| 3 | In-memory node | Apply V2 (HLC=T2) then V1 (HLC=T1 < T2) | Log has both in HLC order; visible value is V2 |
| 4 | In-memory node | `delete("alice")` then `get("alice")` | Returns tombstone marker, not last live value |
| 5 | 3 in-memory nodes A, B, C; all `READY`; `rf=3` | `put` on A, wait quorum | `get` on B and C each return same value |
| 6 | A owns partition P; B does not own P | `put` for P sent to B | B proxies to A; A stores; B returns ack |
| 7 | 3 nodes; C's transport set to error | `put` on A with `rf=3` | Hint stored for C; A+B quorum returns ack; hint carries original HLC |
| 8 | A holds hints for C at T1, T3, T2 | C recovers to `REACHABLE`; A delivers | C receives in HLC order T1, T2, T3 |
| 9 | C classified `DEAD` on A; C actually responds | A attempts `replicate` to C | Operation outcome promotes C to `SUSPECT` then `REACHABLE`; no gossip emitted |
| 10 | Same ring view on 3 nodes | `preference_list` for same `StoreKey` | All 3 return identical ordered list |
| 11 | `DRAINING` node D in preference list; primary write | `put` routed to D | D returns `TEMPORARY_UNAVAILABLE`; coordinator falls through to next member |
| 12 | `DRAINING` node D | `get` sent to D | D serves the read |
| 13 | Property (Hypothesis) | Random sequences of `apply` on in-memory store | Visible value always equals `max(all events for key)` by HLC |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] 3-node in-memory cluster: write on A ack'd → readable from B and C.
- [ ] Node goes `DEAD` → writes hinted → node recovers → hints delivered in HLC order → state matches no-gap scenario.
- [ ] `DRAINING` node serves reads and returns `TEMPORARY_UNAVAILABLE` for primary writes.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Ring topology changes (join/leave partition transfer), gossip protocol details,
`MemberPhase` FSM, node state file. Those are proposals 002 and 004.
