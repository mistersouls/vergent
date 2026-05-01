# Node Join

This document describes the complete sequence that begins when an operator
issues a `join` command to a Tourillon node that is in the `IDLE` phase and
ends when the node transitions to `READY`. It covers both the **first-node
bootstrap** variant (no peers exist yet) and the **subsequent-node join**
variant (at least one seed is reachable). It also describes what happens when
a join attempt fails mid-sequence.

Cross-references:
- `docs/lifecycle/node-startup.md` — how the node reaches `IDLE`
- `docs/lifecycle/node-restart.md` — how a crash during `JOINING` is recovered
- `docs/lifecycle/node-drain.md` — the symmetric leave sequence
- `docs/membership.md` — `MemberPhase` FSM and `Member` value object
- `docs/ring.md` — ring epochs, vnodes, and partition ownership
- `docs/rebalance.md` — the partition transfer protocol used during join

---

## 1. Trigger

The `join` command is the only event that advances a node from `IDLE` to
`JOINING`. A node does not join simply by starting up; an explicit operator
action is always required. This is intentional: it prevents a misconfigured or
partially provisioned node from acquiring partition responsibility before it is
ready.

The command is issued via `tourctl`:

```
tourctl node join --seed <host:port> [--seed <host:port> ...]
```

or by including a `[cluster].seeds` list in the config file and running:

```
tourillon node join
```

At least one seed address must be available, either through the flag or the
config. For the first-node bootstrap variant, no seed is needed (see §3).

---

## 2. IDLE → JOINING Transition

When the `join` command is received, the node performs the following
operations in order before emitting any gossip:

1. Increments its `generation` counter. The `generation` field in the `Member`
   value object is a monotonically increasing integer that advances each time
   the node re-enters the ring from `IDLE`. Any gossip record carrying an older
   `generation` for this `node_id` will be silently discarded by peers once
   the new generation propagates.
2. Resets its `seq` counter to 0.
3. Persists `phase = JOINING`, `generation = <new value>`, and `seq = 0` to
   the local storage backend. The phase is written to durable state before any
   gossip is emitted. This guarantee allows a crash at any point after this
   step to be recovered as a `JOINING` restart (see
   `docs/lifecycle/node-restart.md` §3).
4. Emits an initial gossip record with `phase = JOINING` to any seeds that
   are contactable.

A node that is already in `JOINING` rejects a second `join` command with an
`ALREADY_JOINING` error. Only one join sequence is active at a time.

---

## 3. First-Node Bootstrap (No Seeds)

When a node is configured with no seeds, or when all configured seeds are
unreachable at join time, the node treats itself as the bootstrap node for a
new cluster:

1. The node constructs an empty initial ring with a single entry: its own
   vnodes, derived deterministically from its `node_id` and the configured
   `HashSpace`.
2. It assigns itself all partitions (as there are no other nodes).
3. It publishes ring epoch 1 (the first epoch) containing only its own vnodes.
4. It persists `phase = READY` and emits gossip declaring itself `READY`.

From this point the bootstrap node is fully operational. Subsequent nodes join
by contacting this node as a seed. The bootstrap node's ring epoch advances
each time a new member joins.

This variant produces a single-node cluster. Replication factor `rf` is
honoured best-effort: with only one node, every partition has one replica. New
nodes joining later will trigger the rebalance protocol to bring the replica
count up to `rf` as the cluster grows.

---

## 4. Subsequent-Node Join (Seeded)

When at least one seed is reachable, the joining node and its seeds execute
the following protocol. Steps 1–5 happen in `JOINING` phase; Step 6 is the
transition trigger to `READY`.

### Step 1 — Seed contact and ring view acquisition

The joining node connects to each configured seed over mTLS on the seed's
`servers.peer` listener. It sends a `ring.fetch` request and blocks until it
receives a response containing:

- The current ring epoch number.
- The complete ordered set of `(node_id, token)` vnodes in the ring.
- The membership gossiped by the seed: a map of `node_id → Member` including
  each peer's `phase`, `generation`, and `seq`.

If no seed responds within the configured timeout the join attempt is aborted.
The node remains in `JOINING`, persists a failure event, and retries with
exponential backoff up to the configured `join_retry_max`. If the maximum
retries are exhausted the node logs a fatal error and the join procedure is
suspended until the operator retries the command or restarts the node.

The node that provides the ring view does not need to be `READY`. A `DRAINING`
seed produces a valid ring view. An `IDLE` or `JOINING` seed returns an error
and the joining node tries the next seed.

### Step 2 — Vnode computation and ring insertion

The joining node computes its own vnode set deterministically from its
`node_id` and the cluster's `HashSpace` configuration. It sends a
`ring.propose_join` message to the current ring members, carrying its vnodes
and its `node_id`.

Because Tourillon is leaderless, there is no single node that "approves" the
join. Instead, every existing node independently computes the new ring epoch
(`E_old + 1`) that would result from adding the newcomer's vnodes. The gossip
layer propagates the new ring view; any node that receives a ring view at
`E_old + 1` containing the newcomer's vnodes adopts it as the current epoch.

### Step 3 — Partition assignment computation

Once the new ring epoch `E_new` has propagated, each node — including the
joiner — independently computes the ownership delta `Δ(E_old → E_new)`: the
set of partitions whose primary or secondary replica sets have changed. The
joiner's assigned partitions are the subset of `Δ` for which it is listed as a
new owner.

Because ownership computation is a pure function of `(E_new, ring)`, every
node derives the same delta without coordination. The joining node knows
exactly which partitions it will own before any data has moved.

### Step 4 — Data handoff from source nodes

For each partition assigned to the joining node, the corresponding source
nodes (previous owners at `E_old`) initiate a `rebalance.transfer` as
described in `docs/rebalance.md`. The joining node's storage backend accepts
incoming transfer batches in a quarantined staging area and does not serve
reads for those partitions until a `rebalance.commit` is issued.

During this window the joining node is in a **dual-ownership** state for
migrating partitions: the source accepts reads and writes; the joiner accepts
writes (which are immediately forwarded to the source for replication) but
returns `TEMPORARY_UNAVAILABLE` for reads on quarantined partitions.

The joining node monitors transfer progress. For each partition, it tracks the
last HLC applied from the transfer batch and compares it against the high-
watermark declared by the source in the `rebalance.plan`. Integrity validation
(digest comparison) is performed before any committed partition transitions out
of quarantine.

### Step 5 — Commit and partition activation

For each transferred partition that passes integrity validation, the joining
node issues a `rebalance.commit` to the source. The source acknowledges and
relinquishes ownership for that partition. On the joining node, the partition
is atomically promoted from quarantine to the live read path.

### Step 6 — JOINING → READY

The joining node transitions to `READY` when all partitions in its primary
replica slots have been committed. Secondary replica slots that are not yet
fully transferred do not block the `READY` transition; they continue
transferring in the background. The join is considered complete when all
primary slots are ready to serve writes.

The node:
1. Persists `phase = READY` and increments `seq`.
2. Emits a gossip record with `phase = READY`.
3. Begins accepting write and read traffic for its owned partitions.

---

## 5. What a Node in JOINING Can and Cannot Do

| Operation | JOINING behaviour |
|-----------|-----------------|
| Accept `put` / `delete` for owned partition (quarantined) | Accepted, forwarded to source; stored locally after commit |
| Serve `get` for owned partition (quarantined) | `TEMPORARY_UNAVAILABLE` |
| Accept `put` / `delete` for non-owned partition | Proxied to the correct owner |
| Serve `get` for non-owned partition | Proxied to the correct owner |
| Accept replication traffic | Yes |
| Participate in gossip | Yes — emits `phase = JOINING` |
| Appear in preference lists | No — `JOINING` is ineligible |
| Accept a second `join` command | No — `ALREADY_JOINING` |
| Accept a `leave` command | No — not yet READY |

---

## 6. Concurrent Joins

Multiple nodes may attempt to join simultaneously. Because every node
independently computes ring epochs and rebalance plans as pure functions of the
ring state, concurrent joins are handled correctly by the system's deterministic
plan derivation.

However, to bound rebalance plan churn and data movement, the membership layer
should coalesce simultaneous joins into a small number of epoch transitions
where possible. A cluster operator performing a mass scale-out should admit
nodes in controlled batches rather than issuing all `join` commands
simultaneously.

---

## 7. Abort and Error Paths

| Condition | Behaviour |
|-----------|-----------|
| No seeds reachable within timeout | Retry with backoff; log warning; suspend after `join_retry_max` |
| Seed returns stale epoch | Joiner contacts next seed |
| Source node crashes mid-transfer | Transfer resumes from checkpoint; source's crash triggers hinted handoff |
| Integrity validation fails | Joiner discards partial transfer; requests retransmission |
| Ring epoch superseded during join | Joiner recomputes delta against new epoch; in-progress transfers for stable partitions are unaffected |
| Second `join` command during JOINING | `ALREADY_JOINING` error; no state change |
