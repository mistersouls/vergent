# Ring

## Overview

The ring subsystem is the heart of Tourillon's data placement and routing
machinery. It maps any `StoreKey` to a deterministic, ordered sequence of
replica nodes through three independent abstractions stacked on top of one
another: a configurable hash space, a virtual-node ring, and a fixed logical
partition table. Each abstraction has a precisely bounded responsibility, and
higher-level components — gossip, replication, rebalance — interact only
through the interfaces each layer exposes, never reaching into the internals
of a lower one.

This document is the normative specification for the ring subsystem. It
complements:

- `docs/architecture.md` — high-level placement invariants and the full
  routing path in context.
- `docs/membership.md` — `MemberPhase` lifecycle and its effect on
  preference-list eligibility.
- `docs/rebalance.md` — partition transfer protocol and ring-epoch
  transitions.

---

## 1. Hash Space

The hash space defines a fixed-size circular integer domain into which all
keys and node identifiers are projected. It is the mathematical foundation
upon which all ring computations are built.

### 1.1 Configurable width

The hash-space width `bits` determines the size of the integer domain:

```
MAX = 2**bits
domain: [0, MAX)
```

In production Tourillon uses `bits=128`, giving 2^128 ≈ 3.4 × 10^38
distinct positions. For testing — especially property-based and
model-checking tests — smaller widths (e.g. `bits=8`, giving 256 positions)
dramatically reduce the search space while preserving every structural
property of the ring.

`HashSpace` is not a global singleton. It is instantiated once at cluster
bootstrap with a fixed `bits` value and injected into every component that
needs to compute positions. Two components sharing the same `HashSpace`
instance are guaranteed to operate in a consistent coordinate system.
Mixing `HashSpace` instances with different `bits` values in the same
cluster is a configuration error and must be detected at startup.

### 1.2 Hash function

The hash function `hash(value: bytes) -> int` projects arbitrary byte
sequences into `[0, MAX)`. For `bits=128`, MD5 is used as a deterministic
128-bit mapping function. MD5 is chosen here exclusively for its output
width and determinism, not for any cryptographic guarantee — all
cryptographic security is provided by the mTLS layer. For `bits < 128`,
the output is truncated to `bits` significant bits.

The hash function is deterministic across all nodes and all process
restarts. Two nodes computing `hash(keyspace + key)` for the same inputs
always produce the same token.

### 1.3 Circular arithmetic

All position arithmetic on the hash space is performed modulo `MAX`,
preserving the circular topology. Given any position `x` and offset `d`:

```
add(x, d) = (x + d) % MAX
```

Interval membership on the ring uses a half-open interval `(a, b]`:

- Normal interval (`a < b`): `a < x ≤ b`
- Wrapped interval (`a ≥ b`): `x > a or x ≤ b`

This convention is used throughout: by the `Ring` successor lookup, by the
`Partitioner` segment boundaries, and by the rebalance transfer range
checks.

### 1.4 Token generation

Token generation — both the deterministic `(label, index)` derivation and the
random 64-bit index path used during first-time node provisioning — is the
responsibility of the node bootstrap layer, not the hash space itself. The
hash space exposes only the primitive `hash(value: bytes) -> int` function;
callers are responsible for constructing the input bytes. See
`docs/operations.md` for the provisioning flow.

---

## 2. Virtual Nodes

A virtual node (`VNode`) is the atomic unit of position on the ring. It is
an immutable pair:

```
VNode(node_id: str, token: int)
```

where `token` is an `int` in `[0, 2**bits)`. At `bits=128`, tokens span
`[0, 2**128)`; at `bits=8` (tests), they span `[0, 256)`.

A single physical node is assigned multiple vnodes. This multiplication
serves two purposes:

1. **Load distribution** — with N physical nodes and V vnodes per node, the
   ring contains N×V positions. As N grows toward 10 000 nodes, multiple
   vnodes per node keep the key distribution uniform and prevent any single
   node from owning a disproportionate share of the key space.

2. **Smooth rebalance** — when a node joins or leaves, only its vnodes
   move. A larger vnode count amortises the ownership delta across more
   (smaller) partitions, reducing the volume of data transferred per event.

The vnode count per physical node is a configurable parameter fixed at
cluster bootstrap time. It must be chosen in conjunction with the
`partition_shift` parameter (see §3.2). Token values for a node's vnodes
are derived deterministically from the node's `NodeId` and a per-vnode
ordinal index. The same `NodeId` and the same `HashSpace` always produce
the same set of tokens.

---

## 3. Ring

The ring is an **immutable, sorted sequence** of `VNode` instances ordered
by ascending token value. This sorted structure is the mechanism by which
any token in the hash space is mapped to a responsible node through O(log n)
binary search.

### 3.1 Successor lookup

Given a query token `t`, the responsible vnode is the first vnode whose
token is strictly greater than `t` in the sorted sequence. When `t` is
greater than or equal to the last token in the sequence, the lookup wraps
around to the first vnode, preserving the circular structure:

```
successor(t):
    idx = bisect_right(vnodes, t, key=lambda v: v.token)
    if idx == len(vnodes):
        idx = 0   # wrap-around
    return vnodes[idx]
```

`bisect_right` with a key function gives O(log n) complexity where n is
the total number of vnodes in the ring. For a 10 000-node cluster with 16
vnodes per node, n ≈ 160 000 — a lookup takes roughly 17 comparisons.

### 3.2 Immutability

All ring mutations — adding a set of vnodes, removing all vnodes belonging
to a set of departing `node_id` values — return a new `Ring` instance. The
existing instance is never modified. This design makes it safe to hold a
ring snapshot across an async suspension point: the ring view held by a
coroutine cannot change underneath it while it is suspended.

Adding vnodes uses a merge-sort step (O(k log k + n)) rather than repeated
insertion. Removing all vnodes for a set of departing nodes is a single
linear filter pass (O(n)).

### 3.3 Ring walk

`iter_from(vnode)` yields all vnodes in ring order starting from a given
vnode, wrapping around at the end of the sequence. This traversal is used
by the placement strategy (§5.1) to collect distinct physical nodes for the
preference list and by the rebalance coordinator to enumerate successor
candidates after a topology change.

---

## 4. Partitioner and Logical Partitions

The partitioner imposes a **static, fixed-size grid** of logical partitions
on top of the hash space. Logical partitions are independent of physical
nodes and vnodes. They are the stable unit of data ownership, the unit of
ring-epoch accounting, and the unit of transfer during rebalance.

### 4.1 Partition grid

The `partition_shift` parameter determines the total number of logical
partitions:

```
total_partitions = 2**partition_shift
step             = 2**bits // total_partitions
```

Each partition `pid` covers the half-open interval:

```
(pid × step,  (pid + 1) × step]
```

The mapping from a hash value `h` to its partition identifier is a single
right-shift, equivalent to integer floor-division by `step`:

```
pid = h >> (bits - partition_shift)
```

This is O(1). `partition_shift` must be strictly less than `bits`; a value
equal to or greater than `bits` would produce a step of 0, which is a
configuration error and must be rejected at startup.

### 4.2 Choosing `partition_shift` for large clusters

The partition grid is fixed for the lifetime of a cluster. Changing it
after bootstrap requires a full data migration and is not supported. The
value must therefore be chosen conservatively to accommodate the cluster's
anticipated maximum size.

The central constraint is that there must be enough partitions to distribute
ownership evenly across all nodes. The recommended rule is to target at
least **10× the maximum expected node count** in total partitions. This
ratio ensures that each node owns multiple partitions on average, which
bounds the per-event data movement and keeps the preference-list walk short.

| Max node count | Minimum `partition_shift` | Total partitions |
|---------------:|-------------------------:|-----------------:|
| 100            | 10                       | 1 024            |
| 1 000          | 14                       | 16 384           |
| 10 000         | 17                       | 131 072          |
| 100 000        | 20                       | 1 048 576        |

For a cluster planned to scale to 10 000 nodes, `partition_shift=17` yields
131 072 partitions — approximately 13 partitions per node at full capacity,
which gives smooth distribution and proportionally small data movement per
topology event.

For test instances with `bits=8`, `partition_shift` must be at most 7
(128 partitions maximum). This constraint is enforced at construction time.

### 4.3 LogicalPartition

A `LogicalPartition` is an immutable value object representing one slot in
the partition grid:

```
LogicalPartition(pid: int, start: int, end: int)
```

`LogicalPartition` instances are static: they do not change when the ring
topology changes. They are the unit of ownership in the ring view and the
unit of transfer in the rebalance protocol. The half-open interval
`(start, end]` precisely matches the ring's `in_interval` semantics defined
in §1.3.

`LogicalPartition.contains(h)` returns `True` when hash value `h` falls in
`(start, end]`. This is the canonical membership test used by the storage
layer to verify that a record belongs to the expected partition before
committing a rebalance transfer batch.

### 4.4 PartitionPlacement

`PartitionPlacement` ties a `LogicalPartition` to the `VNode` that currently
owns it on the ring:

```
PartitionPlacement(partition: LogicalPartition, vnode: VNode)
```

It is a derived, ephemeral value. After any ring mutation (vnode add or
remove), the partition placement for affected partitions must be recomputed
from the new ring. `PartitionPlacement` is never persisted; the durable
state is the ring (vnode set + epoch) and the logical partition grid.

`Partitioner.placement_for_token(token, ring) → PartitionPlacement` is the
primary routing entry point exposed by the partitioner. It combines the
partition lookup (`pid = token >> (bits - partition_shift)`), the segment
boundary retrieval, and the `Ring.successor` call into a single step, returning
a fully resolved `PartitionPlacement`. Callers above the partitioner layer
never need to invoke `pid_for_hash` or `segment_for_pid` directly; those remain
available as lower-level helpers for use cases such as integrity checking during
rebalance.

`PartitionPlacement.address` is the primary handle used by storage and
rebalance components to identify a partition's data segment. It returns the
partition identifier in a form suitable as a stable storage key prefix,
allowing a storage implementation to retrieve or scan all records belonging
to a partition without iterating the full hash space.

---

## 5. Placement Strategy and Preference List

The preference list for a given token is the ordered sequence of `NodeId`
values that should hold replicas of the data mapping to that token. The
first entry is the primary replica; subsequent entries are secondary
replicas in ring-successor order. This list is the single input to all
routing decisions for reads, writes, hinted handoff, and rebalance.

### 5.1 Construction algorithm

Starting from `placement.vnode` — the ring owner resolved by
`Partitioner.placement_for_token` — the ring is walked clockwise via
`iter_from`. At each step, a candidate `node_id` is
appended to the preference list only when both of the following conditions
hold:

a. It has **not already been seen** in the current walk — multiple vnodes
   belonging to the same physical node are collapsed into a single entry.
   The preference list contains distinct physical node identifiers, not
   vnode identifiers.

b. The node's **`MemberPhase` is eligible** according to the rules in §5.2.

The walk continues until `rf` distinct eligible node identifiers have been
collected, or the full ring has been traversed exhausting all eligible
candidates. Fewer than `rf` results is a normal condition during bootstrap,
drain, or in very small test clusters; the write and read paths handle
under-populated preference lists gracefully via hinted handoff and
degraded-mode quorum.

The algorithm is a **pure function** of `(placement, ring)` and
produces a deterministic result for identical inputs on every node. No
wall-clock time, no randomness, and no local mutable state influence the
output.

### 5.2 Phase eligibility

The `MemberPhase` of each candidate node is consulted before adding it to
the preference list:

| Phase | Eligible | Rationale |
|-------|:--------:|-----------|
| `IDLE` | No | The node holds no partition data and is not participating in the cluster. Including it would route traffic to a node that will always fail. |
| `JOINING` | No | The node is bootstrapping and does not yet hold a consistent snapshot of its assigned partitions. Including it in normal client routing would expose partial or missing data. The rebalance coordinator tracks the joining node separately for the dual-ownership window, but the preference list is not the right mechanism for that. |
| `READY` | Yes | Fully operational, holds all data for its assigned partitions. |
| `DRAINING` | Yes | Still holds all data and can validly serve reads. For writes, it returns `TEMPORARY_UNAVAILABLE` on its primary slots, so coordinators fall through to the next eligible member in the list. Keeping `DRAINING` nodes in the preference list preserves read availability throughout the entire drain and ensures the dual-ownership write fan-out in the rebalance protocol (see `docs/rebalance.md`) functions correctly. Excluding a draining node would silently remove a valid read replica and would break the rebalance handover protocol. |

A node whose `MemberPhase` is not yet known locally due to gossip lag is
treated as ineligible until a gossip record for that node has been received
and recorded.

### 5.3 PlacementStrategy as a Protocol

`PlacementStrategy` is defined as a `Protocol` rather than a concrete
class. The preference-list construction algorithm is expected to gain a
topology-awareness dimension through node labels (§5.4), and declaring
`PlacementStrategy` as a Protocol allows the concrete strategy to be
injected into the routing and replication layers at construction time
without changing any call site.

The Protocol defines a single method:

```python
def preference_list(
    self,
    placement: PartitionPlacement,
    ring: Ring,
) -> list[str]: ...
```

All implementations must return a deterministic result for identical
`(placement, ring)` inputs and must not perform I/O or carry mutable state.
Phase-eligibility filtering (§5.2) will be incorporated into this signature
once the `Member` type is introduced in a future milestone.

### 5.4 Future: topology-aware anti-affinity via node labels

The `Member` value object, whose specification is in `docs/membership.md` §5,
will carry `node_id`, `peer_address`, `generation`, `seq`, and `phase`. A
subsequent milestone will also add a `labels` field:

```python
labels: dict[str, str]
# e.g. {"topology.tourillon.io/zone": "eu-west-1a",
#        "topology.tourillon.io/rack": "rack-07"}
```

When labels are available, a `TopologyAwarePlacementStrategy` implementation
will enforce anti-affinity: no two replicas in the same preference list
may share the same value for a designated label key. The walk algorithm
in §5.1 gains a third eligibility criterion:

c. Adding this node must not place two replicas in the same **failure
   domain** as defined by the configured label key (e.g. rack,
   availability zone, or region).

When the cluster has fewer distinct label values than `rf`, the constraint
is relaxed gracefully: remaining replica slots are filled with the best
available nodes regardless of label, rather than leaving the preference
list under-populated. A cluster with three nodes all in the same AZ and
`rf=3` must still produce a full-length preference list.

The `labels` field is explicitly reserved for this purpose. Components that
consume `Member` must treat label keys and values as opaque metadata and
must not interpret them outside of the `PlacementStrategy` layer.

---

## 6. Ring Epoch and Versioning

Every distinct ring topology — the ordered set of vnodes and their
associated `node_id` values — is identified by a monotonically increasing
**epoch number**. An epoch changes whenever the vnode set changes:
a node join adds vnodes, a completed drain removes them.

Ring epochs serve two purposes. First, they enable **staleness detection**:
a node receiving a rebalance plan or a gossip ring digest can compare its
local epoch against the sender's and detect discrepancies before acting.
Second, they enable **deterministic plan derivation**: the rebalance plan
for a topology change is a pure function of `(epoch_old, epoch_new)` and
the corresponding ring snapshots. Any two nodes computing this function for
the same epoch pair must produce identical plans. The full normative
specification of the rebalance protocol is in `docs/rebalance.md`.

The ring epoch is distinct from the gossip membership version (`Member.seq`
and `Member.generation`). A gossip update that only changes a node's `phase`
or `seq` without altering the vnode set does not advance the epoch. Only
vnode-set changes do. This asymmetry keeps epoch churn low: a cluster of
10 000 nodes may emit thousands of gossip updates per second without
generating new ring epochs.

**Implementation note**: the `Ring` class is a pure data structure that
carries only the ordered vnode set. It does not store an epoch number.
Epoch tracking is the responsibility of the membership or gossip layer that
manages ring transitions; that layer associates each `Ring` snapshot with an
epoch identifier and advances the counter whenever it calls `add_vnodes` or
`drop_nodes` to produce a new `Ring` instance.

---

## 7. Full Routing Path

The complete path from a `StoreKey` to a preference list, showing how each
layer contributes:

```
StoreKey(keyspace, key)
    │
    ▼
HashSpace.hash(keyspace + key)                    → token: int ∈ [0, 2**bits)
    │
    ▼
Partitioner.placement_for_token(token, ring)      → placement: PartitionPlacement
    │                                               (= LogicalPartition + owner VNode)
    ▼
PlacementStrategy.preference_list(                → [str, ...] length ≤ rf
    placement=placement,
    ring=ring,
)
```

The first entry in the returned list is the primary replica; subsequent
entries are secondary replicas. This list is the only input to routing
decisions — no component below the placement layer makes decisions about
which node to contact.
