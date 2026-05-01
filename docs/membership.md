# Membership Model

This document is the normative specification for cluster membership in
Tourillon. It describes the `MemberPhase` finite-state machine, the local
failure-detection invariant, phase persistence across restarts, and the
relationship between the two concepts.

---

## 1. Fundamental Distinction

Tourillon separates two orthogonal concerns that are often conflated in
distributed membership systems:

| Concern | Owner | Scope | Propagated? |
|---------|-------|-------|-------------|
| **Operational phase** | The node itself | Self-declared intent | Yes, via gossip |
| **Local reachability** | Each peer independently | Private observation | No |

A node's operational phase is what it *says it is doing*. Local reachability
is what a peer *observes from its own interaction history*. These are never
mixed, and local reachability is never transmitted.

---

## 2. MemberPhase FSM

`MemberPhase` is the self-declared operational state that a node propagates to
all peers via gossip. It represents the node's current role in the cluster.

### States

| Phase | Meaning |
|-------|---------|
| `IDLE` | Not participating in the cluster. Initial state on first start; also the state reached after a successful drain. |
| `JOINING` | Actively bootstrapping: fetching ring state, receiving handoffs, and preparing to own its assigned partitions. |
| `READY` | Fully operational: owning partitions, serving reads and writes, participating in replication. |
| `DRAINING` | Vacating: transferring partition ownership and completing all in-flight operations before leaving. |

### Transitions

```
IDLE  ──[ join command ]──►  JOINING  ──[ bootstrap done ]──►  READY
                                                                  │
                                                      [ leave command ]
                                                                  │
                                                                  ▼
                                                            DRAINING  ──[ drain done ]──►  IDLE
```

- `IDLE → JOINING`: triggered by an explicit operator `join` command. A node
  does not join simply by starting up.
- `JOINING → READY`: triggered internally when the node has received its
  partition assignments, completed any necessary data handoffs, and is ready
  to serve traffic.
- `READY → DRAINING`: triggered by an explicit operator `leave` command. Once
  entered, `DRAINING` is irreversible for the current cycle.
- `DRAINING → IDLE`: triggered internally when all partitions have been
  transferred, all in-flight operations have settled, and all pending hinted
  handoffs have been forwarded. The node does not transition to `IDLE` until
  the drain is fully complete.

### Invariants

- Phase transitions occur only when the corresponding operations complete,
  never as a side effect of a process restart.
- A node in `DRAINING` does not accept new writes for the partitions it is
  vacating, but it may still proxy those writes to other ring members.
- Cancelling a drain is not supported. A node that wishes to re-join after
  initiating a drain must complete the drain first, then issue a new `join`
  command.

---

## 3. Phase Persistence

The current phase is persisted to local durable storage before any gossip
carrying that phase is emitted. This guarantees that a restarted node resumes
the phase it held at shutdown:

- A node that restarts in `READY` announces `READY` immediately and resumes
  serving its partition assignments.
- A node that restarts in `DRAINING` resumes the drain from the checkpoint
  recorded before the restart.
- A node that restarts in `JOINING` resumes the bootstrap sequence.
- A node that restarts in `IDLE` does nothing until an explicit `join` command
  is issued.

---

## 4. Local Reachability (Failure Detection)

Each node maintains a private, per-peer reachability classification that is
never propagated. The classifications are:

| Classification | Meaning |
|----------------|---------|
| `REACHABLE` | Operations to this peer have been succeeding. |
| `SUSPECT` | One or more recent operations to this peer have failed. |
| `DEAD` | The peer has been unresponsive for long enough to be treated as permanently unreachable for routing purposes. |

### Detection Trigger

Reachability is determined exclusively by operation outcomes. There is no
heartbeat timer that independently transitions a peer to `SUSPECT`. Instead:

- A failed write, replicate, probe, or any other inter-node operation raises
  the suspicion level of the target peer on the local node.
- A subsequent successful operation lowers the suspicion level.

This means two nodes may hold different local reachability views of the same
peer at the same instant, and that is correct. There is no global agreement on
reachability, only convergence of the shared `MemberPhase` data.

### Isolation from MemberPhase

Local reachability classifications (`REACHABLE`, `SUSPECT`, `DEAD`) are:

- Computed independently by each node from its own operation history.
- Never included in any gossip message.
- Never exposed in the `Member` value object that is gossiped to peers.
- Kept entirely within the gossip engine's internal state.

A peer classified as `DEAD` locally still appears in the ring view with its
last known `MemberPhase`. Routing and hinted handoff decisions are made based
on the combination of ring membership and local reachability — but only the
ring membership is shared.

---

## 5. Member Value Object

The `Member` value object, to be defined in `tourillon/core/structure/membership.py`,
is the unit of gossip exchange. It will carry:

| Field | Type | Transmitted? | Description |
|-------|------|-------------|-------------|
| `node_id` | `str` | Yes | Stable logical identifier. |
| `peer_address` | `str` | Yes | mTLS gossip endpoint (`host:port`). |
| `generation` | `int` | Yes | Monotonically increasing restart counter. |
| `seq` | `int` | Yes | Monotonically increasing version counter. Incremented on every gossip emission; used only for merge ordering, not failure detection. |
| `phase` | `MemberPhase` | Yes | Self-declared operational phase. |

The `supersedes()` method on `Member` determines merge precedence during gossip
reconciliation: higher `generation` wins; equal generation defers to higher
`seq`. A node bumps its `seq` whenever it changes phase, so phase updates
propagate as naturally more-recent records.

---

## 6. Relationship to the Ring

Membership and ring topology are complementary but distinct views of cluster
state. Understanding their interaction is essential for implementing correct
routing and rebalance logic.

### Phase transitions and ring epochs

Not every phase transition produces a new ring epoch. Only transitions that
alter the set of virtual nodes (vnodes) in the ring advance the epoch:

| Transition | Ring epoch change? | Reason |
|---|:---:|---|
| `IDLE → JOINING` | Yes | The joining node's vnodes are added to the ring. A new epoch is published via gossip so that all nodes can begin deriving the updated ownership map. |
| `JOINING → READY` | No | The vnode set was already committed at the `JOINING` epoch. `READY` confirms the node is operational but introduces no new topology change. |
| `READY → DRAINING` | No | The draining node's vnodes remain in the ring throughout the entire drain so that the rebalance coordinator can route transfers correctly and the dual-ownership window functions as specified in `docs/rebalance.md`. |
| `DRAINING → IDLE` | Yes | The drain is complete; the node's vnodes are removed from the ring, producing a new epoch. |

This asymmetry is intentional. Ring epoch changes are expensive — they
trigger rebalance plan derivation on every node in the cluster. Phase
changes are cheap — they update gossip records only. A cluster of 10 000
nodes may emit thousands of `seq`-incrementing gossip records per second
without generating new ring epochs.

### generation, seq, and epoch propagation

`Member.generation` and `Member.seq` are the merge-precedence keys for
gossip records. They are independent of the ring epoch:

- `generation` advances each time a node re-joins from `IDLE`. It ensures
  that gossip records from a previous incarnation are silently discarded
  when the node rejoins under the same `NodeId`.
- `seq` advances on every gossip emission. It orders concurrent records
  about the same node within the same generation.

Neither `generation` nor `seq` encodes ring-epoch information. The ring
epoch is carried separately in the ring-state gossip payload, not inside
the `Member` value object. A component that needs to reason about topology
change must consult the ring epoch, not `Member.seq`.

### Phase eligibility in placement decisions

The `PlacementStrategy` consults `Member.phase` when building a preference
list. The eligibility rules are defined normatively in `docs/ring.md` §5.2
and summarised here:

| Phase | Eligible for preference list | Summary |
|-------|:----------------------------:|---------|
| `IDLE` | No | Holds no data; not participating. |
| `JOINING` | No | Incomplete data; excluded from normal client routing. Tracked separately by the rebalance coordinator. |
| `READY` | Yes | Fully operational. |
| `DRAINING` | Yes | Still holds all data; valid for reads. Returns `TEMPORARY_UNAVAILABLE` for primary writes so coordinators fall through to the next eligible member. Must remain in the preference list to preserve read availability during drain and to support the dual-ownership write fan-out. |

Excluding `DRAINING` nodes from the preference list would silently drop a
valid read replica for the entire duration of the drain and would violate
the dual-ownership protocol defined in `docs/rebalance.md`.

### Reserved: topology labels

`Member` will gain a `labels` field in a subsequent milestone to support
topology-aware placement — rack or availability-zone anti-affinity. The
field is not included in the initial planned field set. Components that
consume `Member` must treat the fields listed in §5 as the complete set for
this milestone and must not add surrogate fields as a workaround for topology
information. See `docs/ring.md` §5.4 for the full anti-affinity design and
the rationale for reserving this extension point.
