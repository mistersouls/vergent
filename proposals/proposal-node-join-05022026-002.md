# Proposal: Node Join (`tourctl node join`)

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Draft
**Date:** 2026-05-02
**Sequence:** 002
**Schema version:** 1

---

## Summary

This proposal specifies everything required for a node to join a Tourillon
cluster: the consistent-hashing ring that maps every key to a deterministic
replica set, the gossip protocol that propagates membership changes, the two
join paths (first-node bootstrap vs. seeded entry into an existing cluster),
the partition data transfer protocol, the retry and deadline model, and the
complete failure and recovery paths. It is fully self-contained: all ring
and gossip internals needed to understand and implement the join operation are
defined here.

---

## Motivation

A node in `IDLE` holds no data and serves no traffic. Joining the cluster is
the most consequential lifecycle transition and the one most likely to go
wrong in a distributed setting. Several hard problems must be solved at once:

**On the ring side:** every node in the cluster must independently compute the
same ownership map for every key. If two nodes disagree on who owns a
partition, writes silently diverge. The ring must therefore be a pure
deterministic function of a shared state — no coordinator, no randomness, no
wall-clock tiebreaks.

**On the gossip side:** the new ring topology produced by the join must
propagate to every node before any routing decision is made against it. Gossip
must converge in O(log n) rounds, scale to 10 000 nodes, and never produce
conflicting ring views.

**On the data transfer side:** the joining node must receive a consistent
snapshot of the key partitions it will own. A crash mid-transfer must not
produce partial data, and already-transferred partitions must not be re-sent
on restart.

**On the identity side:** `Member.generation` must be incremented exactly once
per join intent, regardless of retries and crashes; peers must be able to
discard gossip from stale incarnations of the same `node_id`.

---

## CLI contract

### `tourctl node join`

Connects to the target node's `servers.peer` listener over mTLS and triggers
the join protocol. Blocks until `READY`, `FAILED`, or an unrecoverable error.

**First-node bootstrap — no seeds configured, no `--seed` flags:**

```
$ tourctl node join
Connecting to node-1.example.com:7701 ...
✓  node-1 is JOINING  (generation 1)
   No seeds configured and no --seed flags → single-node bootstrap
   Assigning all 1 024 partitions to node-1 …
✓  Ring epoch 1 published
✓  node-1 is READY
   KV listener:  node-1.example.com:7700
   Peer listener: node-1.example.com:7701
```

**Seeded join — joining a 2-node cluster:**

```
$ tourctl node join --seed node-1.example.com:7701 \
                    --seed node-2.example.com:7701
Connecting to node-3.example.com:7701 ...
✓  node-3 is JOINING  (generation 1)
   Contacting seeds …
     node-1.example.com:7701  ✓  epoch 3
   Fetching ring snapshot …
   Computing partition assignment for node-3 …
     240 total partitions  →  80 assigned to node-3
   Receiving partition data:
     [██████████░░░░░░░░░░]  40 / 80 partitions committed
     [████████████████████]  80 / 80 partitions committed
   Broadcasting join notice …
✓  Ring epoch 4 published  (node-3 added, 3-node cluster)
✓  node-3 is READY
```

**`--seed` override semantics:** `--seed` flags override `[cluster].seeds`
from the config file for this invocation only. The config file is never
modified. Seeds are tried in the order given.

**Seeds configured but none reachable — deadline expired:**

```
$ tourctl node join
Connecting to node-3.example.com:7701 ...
✓  node-3 is JOINING  (generation 1)
   Contacting seeds …
     node-1.example.com:7701  ✗  connection refused
     node-2.example.com:7701  ✗  timeout (10s)
   Retry 1 in 2s  (deadline: 116s remaining, attempt 1/∞)
   Retry 2 in 4s  (deadline: 110s remaining, attempt 2/∞)
   …
✗  Join deadline exceeded (120s).
   node-3 is FAILED  (failed_from=JOINING).
   Committed: 0 / 80 partitions (none to skip on resume).
   Recovery:  tourctl node join [--seed host:port]
   exit code 1
```

**Recovery from `FAILED` (failed_from=JOINING):**

```
$ tourctl node join --seed node-1.example.com:7701
Connecting to node-3.example.com:7701 ...
   Resuming join  (generation 1 unchanged — fresh deadline 120s)
   Contacting seeds …
     node-1.example.com:7701  ✓  epoch 3
   …
✓  node-3 is READY
```

**Invalid phase responses:**

```
$ tourctl node join   # node is READY
✗  node-3 is READY — already a cluster member.
   Use `tourctl node leave` to drain first.  exit code 1

$ tourctl node join   # node is already JOINING
✗  node-3 is already JOINING.
   Monitor with `tourctl node inspect`.  exit code 1

$ tourctl node join   # node is FAILED with failed_from=DRAINING
✗  node-3 is FAILED after a drain (failed_from=DRAINING).
   Use `tourctl node leave` to resume.  exit code 1
```

---

## Design

### 2.1 Hash space

`HashSpace(bits: int = 128)` defines the circular integer domain `[0, 2**bits)`.

- In production: `bits=128`, giving 2^128 ≈ 3.4 × 10^38 positions.
- In tests: `bits=8` (256 positions) preserves every structural invariant while
  making property-based tests tractable.

**Hash function:** MD5, used exclusively for its 128-bit deterministic output.
Not a cryptographic primitive — all security is provided by mTLS. Output is
truncated to `bits` significant bits for `bits < 128`.

**Circular arithmetic:** all position arithmetic is `mod 2**bits`. Intervals
are half-open `(a, b]` with wrap-around: when `a ≥ b`, the interval crosses
the 0 boundary (`x > a or x ≤ b`).

`HashSpace` is never a global singleton. It is instantiated once at cluster
bootstrap with a fixed `bits` value and injected into every component. Mixing
instances with different `bits` values in the same cluster is a configuration
error caught at startup.

### 2.2 Virtual nodes

```python
@dataclass(frozen=True)
class VNode:
    node_id: str
    token:   int   # ∈ [0, 2**bits)
```

A physical node contributes `node_size.token_count` vnodes to the ring
(`[node].size` in config: `XS`=1, `S`=2, `M`=4, `L`=8, `XL`=16, `XXL`=32;
fixed at cluster join time).
Two purposes:

- **Load distribution** — avoids the "big shard" problem where unlucky token
  placement gives one physical node a disproportionate key-space share.
- **Smooth rebalance** — when a node joins or leaves, only its vnodes move.
  More vnodes means smaller individual transfers, amortising per-event cost.

Token values are derived deterministically:

```python
token = HashSpace.hash(f"{node_id}:{index}".encode())
# for index in range(node_size.token_count)
```

Same inputs → same tokens on every node, guaranteed.

### 2.3 Ring

`Ring` is an **immutable sorted sequence** of `VNode` instances ordered by
ascending token. Immutability is the key invariant: a coroutine holding a ring
snapshot across an `await` cannot have the ring change underneath it.

**Successor lookup — O(log n):**

```python
def successor(self, token: int) -> VNode:
    idx = bisect_right(self._vnodes, token, key=lambda v: v.token)
    if idx == len(self._vnodes):
        idx = 0   # wrap-around
    return self._vnodes[idx]
```

For 10 000 nodes × 16 vnodes = 160 000 entries, this is ~17 comparisons.

**Mutations return new Ring instances:**

```python
ring2 = ring.add_vnodes(new_vnodes)   # merge-sort, O(k log k + n)
ring3 = ring.drop_nodes({"node-3"})  # linear filter, O(n)
```

`ring.iter_from(vnode)` yields all vnodes in clockwise ring order from a given
position, wrapping at the end. Used by the placement strategy and rebalance.

### 2.4 Partitioner and logical partitions

`Partitioner(partition_shift: int)` imposes a **static, fixed-size grid** of
`2**partition_shift` logical partitions. This grid is independent of physical
nodes — it never changes when the ring topology changes.

```
total_partitions = 2 ** partition_shift
step             = 2 ** bits // total_partitions
pid              = h >> (bits - partition_shift)   # O(1)
```

`partition_shift` is fixed for the lifetime of the cluster. Changing it
requires a full data migration and is not supported. Choose conservatively:

| Max node count | Min `partition_shift` | Total partitions |
|---------------:|---------------------:|-----------------:|
| 100            | 10                   | 1 024            |
| 1 000          | 14                   | 16 384           |
| 10 000         | 17                   | 131 072          |

For tests with `bits=8`, `partition_shift` must be at most 7 (128 partitions
maximum). Enforced at construction time.

```python
@dataclass(frozen=True)
class LogicalPartition:
    pid:   int
    start: int
    end:   int   # half-open (start, end]

    def contains(self, h: int) -> bool:
        if self.start < self.end:
            return self.start < h <= self.end
        return h > self.start or h <= self.end   # wrap-around


@dataclass(frozen=True)
class PartitionPlacement:
    partition: LogicalPartition
    vnode:     VNode

    @property
    def address(self) -> str:
        """Stable storage key prefix — e.g. 'p00042' for pid=42."""
        return f"p{self.partition.pid:05d}"
```

`PartitionPlacement` is **ephemeral and never persisted**. After any ring
mutation, placements for affected partitions must be recomputed.

`Partitioner.placement_for_token(token, ring)` is the primary routing entry
point. It combines partition lookup, segment boundary retrieval, and successor
lookup into a single pure O(log n) call.

### 2.5 Placement strategy and preference list

```python
class PlacementStrategy(Protocol):
    def preference_list(
        self,
        placement: PartitionPlacement,
        ring: Ring,
        members: dict[str, Member],
    ) -> list[str]: ...
```

`SimplePreferenceStrategy` — default implementation:

1. Start from `placement.vnode`.
2. Walk clockwise via `ring.iter_from`.
3. At each vnode, append `node_id` to the list only if both:
   a. Not already seen (deduplicate physical nodes that have multiple vnodes).
   b. `members[node_id].phase` is eligible (see table below).
4. Stop when `rf` distinct eligible nodes are collected or the full ring is
   exhausted.

Phase eligibility:

| Phase | Eligible | Reason |
|-------|:--------:|--------|
| `IDLE` | No | No data; not participating. |
| `JOINING` | No | Incomplete snapshot; excluded from client routing. |
| `READY` | Yes | Fully operational. |
| `PAUSED` | No | Everything suspended; not routing-eligible. |
| `DRAINING` | Yes (reads) | Data still present; primary writes return `TEMPORARY_UNAVAILABLE` so coordinators fall through. Must remain eligible to preserve read availability and the dual-ownership window. |
| `FAILED` | No | Inert. |

The result is a **pure function** of `(placement, ring, members)`: deterministic
for identical inputs on every node. No wall-clock time, no randomness.

### 2.6 Ring epoch

Every distinct vnode set is tagged with a monotonically increasing **epoch
number** stored in `[ring].epoch` in the node state file and carried in all
gossip payloads.

| Transition | Epoch advances? | Reason |
|-----------|:---:|---------|
| `IDLE → JOINING` | Yes | Joining node's vnodes added to the ring. |
| `JOINING → READY` | No | Vnode set already committed at JOINING epoch. |
| `JOINING → FAILED` | No | No ownership established; ring unchanged. |
| `READY → PAUSED` | No | Topology unchanged. |
| `READY → DRAINING` | No | Draining node's vnodes remain in the ring throughout. |
| `DRAINING → IDLE` | Yes | Drain complete; draining node's vnodes removed. |
| `DRAINING → FAILED` | No | Drain stalled; ring unchanged until drain completes. |

A node receiving a gossip payload with a higher epoch than its local epoch
fetches the full ring snapshot from the sender before making routing decisions.

### 2.7 Gossip protocol

**Purpose:** propagate `Member` records and ring epoch to every node without
a central coordinator.

**Mechanism: push-pull every `gossip_interval` (default 1s):**

```
Every node, every gossip_interval:
  1. Select `fanout` (default 3) random peers from the membership table.
  2. PUSH: send own Member record + ring_epoch + ring_digest.
  3. PULL: receive peers' Member records + ring states.
  4. For each received Member record m:
       if m.supersedes(local_table[m.node_id]):
           local_table[m.node_id] = m
           if m.ring_epoch > local_ring_epoch:
               request full ring snapshot (ring.fetch)
```

**Convergence:** with `fanout=3` and a cluster of N nodes, gossip reaches all
nodes in O(log N) rounds with high probability.

**A `FAILED` node stops gossiping.** Peers observe silence and progressively
downgrade it: `REACHABLE → SUSPECT → DEAD` via local reachability tracking.

**Gossip payload format:**

```python
@dataclass
class GossipPayload:
    member:      Member
    ring_epoch:  int
    ring_digest: bytes   # SHA-256 of sorted vnode token list; used for fast comparison
    vnodes:      list[VNode] | None  # included when ring_digest changed; else None
```

When `ring_digest` differs from the receiver's local digest, the full vnode
list is included in the payload (push-full) to avoid a separate `ring.fetch`
round-trip for small clusters.

### 2.8 Two join paths

#### Path A — First-node bootstrap

Precondition: **no seeds** in `[cluster].seeds` AND **no `--seed` flags**.

```
1. IDLE → JOINING:
   - Increment generation, reset seq = 0.
   - Write to state file (before gossip).
   - Emit Member gossip: phase=JOINING.

2. Assign all total_partitions partitions to this node's vnodes.

3. Publish ring epoch 1:
   - Write ring.epoch = 1 to state file.

4. JOINING → READY:
   - Write phase=READY, seq=1, ring_epoch=1 to state file.
   - Emit Member gossip: phase=READY.
   - Begin serving kv.put / kv.get / kv.delete.
```

No seed contact. No partition data transfer. No rebalance.

#### Path B — Seeded join

Precondition: seeds exist (from config or `--seed` flags).

Steps are described in detail in §2.9 through §2.12.

### 2.9 Seeded join — ring fetch and vnode proposal

**Step 1: IDLE → JOINING**

```
Acquire state file lock.
Write: generation += 1, seq = 0, phase = JOINING.
Release lock.
Emit Member gossip: phase=JOINING, generation=N, seq=0.
```

The generation increment happens exactly once here. All subsequent retries,
crashes, and `FAILED → JOINING` recoveries for this join lifecycle use the
same generation.

**Step 2: Contact first reachable seed**

```
For each seed address (in order):
  send Envelope(kind="ring.fetch", payload={})
  receive RingFetchResponse:
    vnodes:  list[VNode]     # current full vnode list
    epoch:   int
    members: list[Member]

On first success:
  Update state file: seeds.last_known = [seed_address]
  Store ring snapshot and member records locally.
```

**Step 3: Propose vnode set and compute new ring**

```
# Compute this node's vnodes deterministically
new_vnodes = [
    VNode(node_id, HashSpace.hash(f"{node_id}:{i}".encode()))
    for i in range(node_size.token_count)
]

# Propose join to all known peers via each seed
send Envelope(kind="ring.propose_join", payload={
    "node_id":    node_id,
    "vnodes":     new_vnodes,
    "epoch_from": epoch,
    "generation": generation,
})

# Every node (including joiner) independently computes epoch_to:
epoch_to    = epoch_from + 1
ring_new    = ring_current.add_vnodes(new_vnodes)
# ring_new is the ring after this join completes

# Partition assignment is a pure function:
assigned_partitions = [
    p for p in all_partitions
    if ring_new.successor(p.start).node_id == node_id
]
```

**Step 4: Await ring.propose_join acknowledgements**

Wait for `⌊N/2⌋ + 1` peers (quorum of current cluster size N) to acknowledge
the vnode proposal before proceeding with data transfer. This ensures the new
epoch is visible to a quorum before any partition is vacated.

### 2.10 Partition data transfer

For each partition `p` in `assigned_partitions` not yet in the committed
transfer log:

```
Joiner sends to source (current owner of p):
  Envelope(kind="rebalance.plan.request", payload={"pid": p.pid, "epoch_to": epoch_to})

Source responds with rebalance plan:
  Envelope(kind="rebalance.plan", payload={
      "pid":          p.pid,
      "epoch_from":   epoch_from,
      "epoch_to":     epoch_to,
      "record_count": N,
      "digest":       sha256_hex,   # SHA-256 of all records serialised in HLC order
  })

Source streams transfer batches:
  Envelope(kind="rebalance.transfer", payload={
      "pid":       p.pid,
      "batch_seq": K,
      "records":   [msgpack(r) for r in batch],   # Version and Tombstone records
  })

Joiner stores received records in a quarantined staging area.
  → NOT visible to kv.get until commit.

After all batches received, joiner validates:
  computed_digest = SHA256(all_received_records_in_hlc_order)
  if computed_digest != expected_digest:
      Abort; source retries from rebalance.plan.request.

Joiner promotes quarantined partition to the live read path.
Joiner sends commit:
  Envelope(kind="rebalance.commit", payload={"pid": p.pid, "epoch_to": epoch_to})

Joiner appends (pid, epoch_to) to the committed transfer log
  (persisted alongside state.toml).

Source removes partition p from its active ownership map on receipt of commit.
```

**Concurrent transfers:** bounded by `join_max_concurrent` (default 4) via
`asyncio.Semaphore`.

**Dual-ownership window:** during the transfer of partition `p`, writes for
keys that hash to `p` are accepted by **both** the source node and the joiner
and cross-replicated via `replicate`. This guarantees the joiner's staged data
is always a superset of the source's at commit time. No write is lost during
the handover.

### 2.11 `JOINING → READY` transition

When all assigned partitions have a committed `rebalance.commit` entry in the
transfer log:

```
1. Broadcast join-complete notice:
   Envelope(kind="node.joined", payload={"node_id": node_id, "epoch_to": epoch_to})

2. All peers compute ring_new = ring.add_vnodes(this_node_vnodes).
   All peers write ring.epoch = epoch_to locally.

3. Acquire state file lock.
4. Write: phase=READY, seq=seq+1, ring_epoch=epoch_to.
5. Release lock.
6. Emit Member gossip: phase=READY, generation=unchanged, seq=new_seq.
7. Begin serving kv.put / kv.get / kv.delete.
```

### 2.12 Retry and deadline model

The join operation uses a two-axis budget: a **`max_retries`** cap (hard
attempt count) and a **`deadline`** budget (wall-clock). A failure is terminal
if *either* limit is reached first. Setting `max_retries = -1` makes the
attempt count unlimited (deadline only); setting `deadline` to a sufficiently
large value makes it deadline-only.

**Backoff formula:** `wait = min(backoff_base × 2^attempt, backoff_max)`.
`attempt` resets to 0 at the start of each top-level retry cycle (seed
contact, transfer session). This produces exponential backoff within a session
without permanently saturating the delay.

| `config.toml` key | Default | Meaning |
|-------------------|---------|---------|
| `[join].max_retries` | `-1` | Maximum total retry attempts; `-1` = unlimited |
| `[join].attempt_timeout` | `10s` | Per-attempt timeout (seed contact, single transfer batch) |
| `[join].deadline` | `120s` | Total wall-clock budget from `IDLE→JOINING` write |
| `[join].backoff_base` | `2s` | Initial backoff interval |
| `[join].backoff_max` | `30s` | Maximum backoff interval |
| `[join].max_concurrent` | `4` | Max parallel partition transfers |

The deadline clock is **frozen while `PAUSED`** (see proposal 003 §3.5).
`deadline_remaining_s` and `retries_remaining` in `state.toml` record the
remaining budget at pause time and are restored on resume. Neither the deadline
nor the retry counter advances while the node is paused.

On hitting `max_retries` **or** deadline expiry:

```
1. Acquire state file lock.
2. Write: phase=FAILED, failed_from="JOINING".
3. Release lock.
4. Stop all gossip emission.
5. Log: {event: "join_failed", reason: "deadline_exceeded | max_retries_exceeded",
         generation: N, committed_partitions: K, total: M}
6. All KV and peer handlers return TEMPORARY_UNAVAILABLE.
```

The ring epoch does not change (no partitions were committed to the ring as
this node's primary).

### 2.13 `FAILED → JOINING` recovery

When `tourctl node join` targets a `FAILED` node with `failed_from=JOINING`:

```
1. Verify failed_from = "JOINING". If not, return error.
2. Acquire state file lock.
3. Write: phase=JOINING, seq=seq+1.
   generation is NOT incremented.
4. Release lock.
5. Emit gossip: phase=JOINING, generation=unchanged, seq=new_seq.
6. Load committed transfer log.
7. Restart join from §2.9 Step 2 (ring.fetch) with fresh deadline.
   Skip partitions already in the committed transfer log.
```

### 2.14 Restart from `JOINING`

On process restart with persisted `phase=JOINING`:

```
1. Read state file → generation, seq, ring_epoch, last_known.
2. Emit gossip: phase=JOINING, generation=unchanged, seq=seq+1.
3. Contact cluster via seeds in priority order:
   a. last_known (saved on first successful seed contact)
   b. [cluster].seeds from config
4. Fetch current ring snapshot.
5. Compute assigned_partitions (pure function of new ring).
6. Load committed transfer log.
7. Skip committed partitions.
8. Resume transfers for remaining partitions.
9. Continue to JOINING → READY (§2.11).
```

A fresh `join_deadline` begins from restart time.

---

## Core invariants

- The ring is a **pure function** of its ordered vnode set. Two nodes with the
  same vnode set always derive the same ring, preference lists, and partition
  assignments.
- `partition_shift` is **immutable** after cluster bootstrap.
- `[node].size` is **immutable** after the node joins; the token count it
  implies cannot change without the node leaving and re-joining.
- Ring epoch **never regresses** and advances only when the vnode set changes.
- `PartitionPlacement` is **ephemeral**: never persisted; always recomputed after
  ring mutation.
- `Member.supersedes()` is the **sole merge rule** for gossip records. No
  wall-clock comparison is used during merge.
- `FAILED` nodes **never gossip**. Peers observe silence; local reachability
  downgrades them to `DEAD`.
- `PAUSED` nodes emit **one final gossip record** then stop; the join/drain
  deadline clock is frozen.
- `Member.generation` is incremented **exactly once** per join lifecycle, at
  `IDLE → JOINING`. Never on crash restart or `FAILED → JOINING` retry.
- Quarantined partition data is **never visible** to `kv.get` until
  `rebalance.commit` succeeds.
- The committed transfer log is **append-only** per join lifecycle. A new join
  (after `IDLE → JOINING → READY`) resets the log.
- The node state file is **always written before gossip** is emitted for any
  phase transition.
- Concurrent joins are serialised at the ring epoch quorum step: a joiner's
  `ring.propose_join` is only committed once `⌊N/2⌋ + 1` peers acknowledge it
  with the same `epoch_from`. Two concurrent proposals with the same
  `epoch_from` will each gather at most `⌊N/2⌋` acks before the other's
  commit advances the epoch, forcing the slower joiner to re-fetch and retry.

---

## Design decisions

### Decision: concurrent joins serialised via ring epoch quorum

**Alternatives considered:** a per-cluster join lock held by one node; optimistic
concurrency with rollback.
**Chosen because:** a per-cluster lock is a single point of failure and
introduces a leader-election sub-problem. Epoch-based quorum is already
present for correctness. When two joiners race, the one that reaches
acknowledgement quorum first commits its epoch; the other observes a higher
`epoch_from` on stale peers and automatically re-fetches the new ring state
before retrying. No additional coordination primitive is needed.

### Decision: `max_retries` + deadline as independent limits

**Alternatives considered:** deadline only (no retry cap); retry count only
(no deadline).
**Chosen because:** a deadline alone does not prevent a pathological retry
pattern from hammering a recovering peer. A retry cap alone can expire on a
slow-but-healthy network. Together they bound the worst-case total retries
(`max_retries`) and the worst-case wall-clock impact (`deadline`). Setting
`max_retries = -1` is the safe default for most deployments.

### Decision: ring is a pure function — no coordinator

**Alternatives considered:** a coordinator node pushes partition assignments to
the joiner; a DHT-style negotiation.
**Chosen because:** a coordinator is a single point of failure in a leaderless
system. A pure function eliminates the possibility of two nodes disagreeing on
the assignment and requires no extra round-trip. Any two nodes given the same
ring state compute the same result.

### Decision: two explicit paths, not a fallback chain

**Alternatives considered:** always attempt seed contact; silently fall back to
bootstrap if all seeds fail.
**Chosen because:** "fallback to bootstrap" is a footgun. An operator who
accidentally misconfigures seeds would silently create a diverging single-node
cluster rather than getting a clear error. The paths are intentionally
distinguished: no seeds at all → bootstrap; seeds present but unreachable →
`FAILED` after deadline.

### Decision: immutable `Ring`

**Alternatives considered:** mutable ring protected by a read-write lock.
**Chosen because:** in an async system, a lock must be held across every `await`
that reads the ring — which is every I/O operation. An immutable ring needs no
lock on the read path. The copy-on-write cost for `add_vnodes` / `drop_nodes`
is O(k log k + n), negligible for infrequent join/leave events.

### Decision: quarantined staging area for received partition data

**Alternatives considered:** write directly to the live partition; transaction
log for rollback.
**Chosen because:** a rollback log over an existing storage layer is effectively
a staging area built on top — with added complexity. The staging area is
simple: writes go to a scratch location; `rebalance.commit` promotes atomically.
If the transfer fails the scratch space is discarded; the live partition is
untouched.

### Decision: `generation` invariant — incremented exactly once per join lifecycle

**Alternatives considered:** increment on every `FAILED → JOINING` retry;
increment on every restart.
**Chosen because:** `generation` identifies the node's membership incarnation.
Incrementing on retry would cause peers to treat each retry as a new member
joining, generating unnecessary ring epochs and vnode proposals. One generation
covers all attempts for a single operator-issued `tourctl node join`.

### Decision: gossip push-pull with random fanout

**Alternatives considered:** broadcast flooding; central ring authority.
**Chosen because:** flooding is O(n) per event — unacceptable at 10 000 nodes.
A central ring authority is a single point of failure. Push-pull gossip with
random fanout=3 converges in O(log n) rounds and scales horizontally.

---

## Proposed code organisation

The following files do not yet exist. They will be created during implementation.

```
tourillon/
  core/
    ring/
      __init__.py
      hash_space.py      # HashSpace class
      vnode.py           # VNode dataclass
      ring.py            # Ring (immutable sorted sequence, bisect_right lookup)
      partitioner.py     # Partitioner, LogicalPartition, PartitionPlacement
      placement.py       # PlacementStrategy Protocol, SimplePreferenceStrategy
    gossip/
      __init__.py
      engine.py          # GossipEngine: push-pull loop, Member table, epoch tracking
      payload.py         # GossipPayload dataclass
    handlers/
      join.py            # JoinHandler: tourctl node join on servers.peer
      rebalance.py       # RebalancePlanHandler, TransferHandler, CommitHandler
    ports/
      gossip.py          # GossipPort Protocol (ring.fetch, ring.propose_join)
    structure/
      rebalance.py       # RebalancePlan, TransferBatch, CommittedTransferLog

tourctl/
  core/
    commands/
      node_join.py       # `tourctl node join` CLI entry point
```

---

## Interfaces (informative)

```python
# tourillon/core/ring/hash_space.py
class HashSpace:
    def __init__(self, bits: int = 128) -> None: ...
    def hash(self, value: bytes) -> int: ...


# tourillon/core/ring/ring.py
from bisect import bisect_right
from typing import Iterator

class Ring:
    """Immutable sorted sequence of VNode instances.

    Mutations (add_vnodes, drop_nodes) return new Ring instances; the
    original is never modified. This makes it safe to hold a ring snapshot
    across any number of await points.
    """

    def successor(self, token: int) -> VNode: ...
    def add_vnodes(self, vnodes: list[VNode]) -> "Ring": ...
    def drop_nodes(self, node_ids: set[str]) -> "Ring": ...
    def iter_from(self, vnode: VNode) -> Iterator[VNode]: ...
    def __len__(self) -> int: ...


# tourillon/core/ring/partitioner.py
class Partitioner:
    def __init__(self, hash_space: HashSpace, partition_shift: int) -> None: ...

    def placement_for_token(self, token: int, ring: Ring) -> PartitionPlacement: ...
    def pid_for_hash(self, h: int) -> int: ...
    def segment_for_pid(self, pid: int) -> LogicalPartition: ...


# tourillon/core/ring/placement.py
from typing import Protocol

class PlacementStrategy(Protocol):
    def preference_list(
        self,
        placement: PartitionPlacement,
        ring: Ring,
        members: dict[str, "Member"],
    ) -> list[str]: ...


# tourillon/core/structure/rebalance.py
@dataclass(frozen=True)
class RebalancePlan:
    pid:          int
    epoch_from:   int
    epoch_to:     int
    record_count: int
    digest:       bytes   # SHA-256 of all records serialised in HLC order


@dataclass
class CommittedTransferLog:
    """Append-only log of committed (pid, epoch_to) pairs.

    Persisted alongside state.toml. Loaded on restart to skip
    already-committed partitions.
    """
    entries: list[tuple[int, int]]

    def is_committed(self, pid: int, epoch_to: int) -> bool: ...
    async def record(self, pid: int, epoch_to: int) -> None: ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | `Ring(bits=8)`, 3 nodes 2 vnodes each | `add_vnodes(new)` | Returns new `Ring`; original unchanged |
| 2 | `Ring(bits=8)` | `drop_nodes({"node-3"})` | New ring without node-3 vnodes; original unchanged |
| 3 | `Partitioner(bits=8, ps=4)` | `placement_for_token` for 100 tokens on 3 identical ring instances | All 3 return identical `PartitionPlacement` |
| 4 | `bits=8, ps=4` vs `bits=128, ps=4` equivalent nodes | Same `StoreKey` | Identical `preference_list` from both |
| 5 | 5 nodes A=READY, B=JOINING, C=READY, D=DRAINING, E=PAUSED; `rf=3` | `preference_list` for key walking all 5 | List = [A, C, D]; B, E excluded |
| 6 | In-memory node, `IDLE`, no seeds | `tourctl node join` | `JOINING` → `READY`; epoch=1; `generation=1` |
| 7 | A=`READY`, B=`IDLE`; B seeded with A | `tourctl node join` on B | B: `JOINING` → `READY`; epoch=2; A and B: identical `preference_list` |
| 8 | B with seed=A unreachable; `join.deadline=5s` | `tourctl node join` on B | B: `FAILED`; `failed_from=JOINING`; gossip stops |
| 9 | B in `FAILED` (`failed_from=JOINING`) | `tourctl node join --seed A` | Same generation; B: `JOINING` → `READY` |
| 10 | B joining A; B crashes after committing partitions 0–39 | Restart B | 0–39 not re-transferred; 40–79 resumed; B reaches `READY` |
| 11 | Write to partition P while P transferred from A to B | `kv.put` on A | Both A (source) and B (joiner) receive write; no loss at commit time |
| 12 | Two gossip rounds between A and B | Exchange Member records | Each node has up-to-date `Member` for the other; `supersedes()` correctly applied |
| 13 | B joining A; B proposal arrives at A | A computes ring after proposal | A derives identical ring to B independently |
| 14 | Prop. (Hypothesis) | N seeded joins on `bits=8` cluster | All surviving nodes derive identical `preference_list` for every token |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `tourillon node start → tourctl node join` (no seeds) → `READY` in one terminal session.
- [ ] Seeded join: B joins A → both `READY` → identical `preference_list` for any `StoreKey`.
- [ ] `tourctl node join` with unreachable seed → `FAILED`; `tourctl node join --seed <reachable>` → `READY` (same generation).
- [ ] Crash mid-seeded-join → restart → resumes without re-transferring committed partitions.
- [ ] `generation` is unchanged across all retries and restarts within one join lifecycle.
- [ ] Property-based tests pass for arbitrary join sequences.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Node leave/drain protocol, pause/resume, KV data plane internals beyond routing.
Those are proposals 004, 005, and 006.
