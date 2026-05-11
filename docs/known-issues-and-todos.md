# Known Issues and Future Work

## Issue: Lost `state.toml` on a Previously-Joined Node

**Status:** Out of scope for current proposals — tracked here for future work.

---

### Problem Description

If `state.toml` is deleted (or lost due to disk failure) on a node that had
already completed or started a join lifecycle, the node loses its persisted
`generation` and `seq` counters. This creates a subtle inconsistency with the
rest of the cluster.

---

### Scenario

1. Node `node-2` completed `IDLE → JOINING` and reached state
   `(phase=JOINING, generation=1, seq=1, tokens=[T0, T1, T2, T3])`.
2. `state.toml` is deleted (operator error, disk wipe, etc.).
3. Node restarts — `FileStateAdapter.load()` returns `None`.
4. `_run_phase()` synthesises a default state:
   `NodeState(phase=IDLE, generation=0, seq=0, tokens=(), epoch=0)`.
5. The node starts in **IDLE with seeds**, waiting for `tourctl node join`.

At this point every peer still holds the old registry entry:
`(node-2, gen=1, seq=1, phase=JOINING, tokens=[T0, T1, T2, T3])`.

---

### What Happens After `tourctl node join`

`NodeJoinHandler` reads `persisted.generation = 0` (the synthesised state) and
computes:

```
new_generation = 0 + 1 = 1
new_seq        = 1
```

The new `NodeState` is `(phase=JOINING, generation=1, seq=1, tokens=[NEW…])`.

However, `Member.supersedes()` uses strict ordering:

```python
(1, 1) > (1, 1)  →  False
```

The new record **does not supersede** the old record already stored on peers.
`MemberRegistry.upsert()` returns `False` — the new record is **silently
rejected**. Peers keep the stale record with the **old token set**.

---

### Consequence

| Situation | Effect |
|-----------|--------|
| Peers retain `gen=1, seq=1` with **old tokens** | The node rejoins with different tokens → ring inconsistency |
| Node broadcasts `gen=1, seq=1` (identical version) | Rejected silently (`accepted=0`, no re-propagation) |
| Node advances to READY (`seq=2`) | `(1, 2) > (1, 1)` → **True** → record supersedes; peers converge |

The risk window is confined to the **JOINING phase**. As soon as the node
transitions to READY (`seq` is incremented again), its record supersedes the
stale one and peers update via gossip. Until that point, peers may route
requests for partitions assigned to the old token set to this node — but the
node does not own those partitions under the new token set, leading to missed
reads or misrouted writes.

---

### Invariant at Risk

> **Generation increment invariant** (from `copilot-instructions.md`):
> `Member.generation` is incremented **exactly once** per `IDLE → JOINING`
> transition. Never on retry, crash-restart, or any other event.

Deleting `state.toml` effectively forces a second `gen=1` incarnation, which
violates the spirit of this invariant even though the letter of the rule (single
increment per `node.join` receipt) is technically preserved.

---

### Current Mitigation

- **Do not delete `state.toml`** on a node that has ever joined the cluster.
- If the file must be restored, copy it from a backup or reconstruct it
  manually with the correct `generation` and `seq` values.

---

### Proposed Fix (Future Work)

Two complementary approaches should be considered:

#### Option A — Cluster-side state query

Before accepting a `node.join`, the joining node (or `tourctl`) queries any
live seed for the current registry entry of its own `node_id`. If a record
exists with `generation ≥ 1`, the join handler uses `existing_generation + 1`
instead of `local_generation + 1`. This ensures the new record always
supersedes the stale one.

**Constraint:** must not contradict the single-increment invariant —
the increment happens once at join time, driven by the authoritative
cluster-side value rather than the (potentially lost) local value.

#### Option B — Monotonic generation file

Store the `generation` counter in a separate, append-only file
(e.g. `generation.lock`) that is **never** deleted by normal operations.
`state.toml` may be deleted for recovery without losing the generation baseline.

---

### Related Proposals

- `proposal-ring-first-node-08022026-002.md` — `generation` and `seq` semantics,
  `NodeState`, `FileStateAdapter`.
- `proposal-gossip-seeded-join-05102026-004.md` — `IDLE → JOINING` transition,
  `supersedes()` merge rule, single-increment invariant.
