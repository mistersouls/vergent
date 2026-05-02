# Proposal: Node Leave (`tourctl node leave`)

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Draft
**Date:** 2026-05-02
**Sequence:** 004
**Schema version:** 1

---

## Summary

This proposal specifies everything that happens when an operator issues
`tourctl node leave`: the `READY → DRAINING` transition, the source-initiated
partition drain protocol, the dual-ownership handover window, drain completion
and ring epoch advancement, the deadline/retry model, the `DRAINING → FAILED`
failure path, `FAILED → DRAINING` recovery, and restart semantics mid-drain.
It is the authoritative reference for all code implementing or testing the
leave operation.

---

## Motivation

A node that stores data cannot simply stop. Its partitions must be handed off
to the remaining nodes so no data is lost and the cluster maintains its
replication factor. The drain protocol must handle three failure modes that
are likely in practice:

1. **Crash mid-drain**: some partitions have already been transferred and
   committed; they must not be re-sent on restart.
2. **Target node temporarily unreachable**: the source should retry with
   backoff rather than failing the entire drain immediately.
3. **Deadline exceeded**: an operator needs a clear signal that the drain
   stalled, and a way to resume it with a fresh deadline.

Additionally, the draining node must remain a valid read replica throughout the
entire drain (data is still there), while signalling to write coordinators that
they should not use it as a primary (the data is being vacated). This is the
dual-ownership window.

---

## CLI contract

### `tourctl node leave`

Connects to the target node's `servers.peer` listener and triggers the drain
protocol. Blocks until `IDLE` or until an error causes it to terminate.

**Happy-path drain:**

```
$ tourctl node leave
Connecting to node-3.example.com:7701 ...
✓ node-3 is DRAINING  (ring topology unchanged during drain)
  Deriving rebalance plan ...
    80 partitions to transfer
    Targets: node-1 (40 partitions), node-2 (40 partitions)

  Transferring to node-1:
    Partitions  0– 39  [████████████████████]  40/40 committed

  Transferring to node-2:
    Partitions 40– 79  [████████████████████]  40/40 committed

  Broadcasting leave notice ...
✓ Ring epoch 5 published  (node-3 removed)
✓ node-3 is IDLE
```

**Drain stalls — deadline exceeded:**

```
  Transferring to node-2:
    Partitions 40– 79  [██████████░░░░░░░░░░]  20/40 committed
    Transfer error: node-2.example.com:7701 timeout
   Retry 2 in 4s  (deadline: 255s remaining, attempt 2/∞)
    ...
✗ Drain deadline exceeded (300s).
  node-3 is now FAILED  (failed_from=DRAINING).
  Already committed: 60/80 partitions (not re-sent on resume).
  Recovery:
    tourctl node leave
  exit code 1
```

**Resume drain from checkpoint:**

```
$ tourctl node leave
Connecting to node-3.example.com:7701 ...
  Resuming drain  (fresh deadline 300s)
  Already committed: 60/80 partitions (skipping)
  Transferring to node-2:
    Partitions 60– 79  [████████████████████]  20/20 committed
  Broadcasting leave notice ...
✓ Ring epoch 5 published  (node-3 removed)
✓ node-3 is IDLE
```

**Irreversibility enforced:**

```
$ tourctl node join   # while DRAINING
✗ node-3 is DRAINING. A drain cannot be cancelled.
  Monitor progress with `tourctl node inspect`.
  exit code 1

$ tourctl node leave  # while already DRAINING
✗ node-3 is already DRAINING.
  Monitor progress with `tourctl node inspect`.  exit code 1

$ tourctl node pause  # while DRAINING
Connecting to node-3.example.com:7701 ...
  node-3 is DRAINING (60/80 partitions committed)
  Suspending drain — deadline frozen at 218.7s remaining
✓ node-3 is PAUSED  (paused_from=DRAINING)
  Resume with:  tourctl node resume
```

**Wrong phase — FAILED from JOINING:**

```
$ tourctl node leave  # FAILED with failed_from=JOINING
✗ node-3 is FAILED after a join (failed_from=JOINING).
  Use `tourctl node join [--seed ...]` to recover.  exit code 1
```

---

## Design

### 4.1 `READY → DRAINING` transition

```
1. Acquire state file lock.
2. Write: phase = DRAINING, seq = seq + 1.
3. Release lock.
4. Emit Member gossip: phase=DRAINING, seq=new_seq, generation=unchanged.
5. Begin drain protocol (§4.2).
```

Steps 2–3 happen before step 4 (write-before-gossip). After step 4, peers
learn that the node is draining and update their preference lists accordingly:
reads are still routed to this node (data is still present), but write
coordinators fall through to the next eligible member.

A `PAUSED` node transitions directly from `PAUSED → DRAINING` using the same
steps above.

### 4.2 Drain protocol (source-initiated)

The drain protocol is the mirror image of the seeded join's partition transfer
(proposal 003), but source-initiated: the draining node pushes data to targets
rather than targets pulling from the source.

**Step 1: Derive rebalance plan**

```python
# Compute the ring as it will look after this node's vnodes are removed:
ring_without_self = ring.drop_nodes({node_id})
epoch_to = ring_epoch + 1

# For each partition currently owned by this node as primary:
for partition in owned_partitions:
    # The new owner is the first eligible node in ring_without_self
    # for partition's token range
    target = ring_without_self.successor(partition.start)
    plans.append(RebalancePlan(partition, epoch_from, epoch_to,
                               record_count, digest))
```

This is a pure function of `(current_ring, node_id, epoch_from)` and produces
the same result on every node independently.

**Step 2: Transfer loop**

For each partition `p` not yet in the committed transfer log:

```
Draining node sends to target:
  1. Envelope(kind="rebalance.plan", payload=RebalancePlan for p)
  2. Stream batches: Envelope(kind="rebalance.transfer", payload=TransferBatch)
     Records are streamed in HLC order.

Target receives and stages:
  3. Store received records in quarantine (not visible to kv.get).

Target validates and commits:
  4. Compute SHA-256 of received records.
     If digest mismatch → abort; draining node retries from step 1.
  5. Promote quarantined partition to live read path.
  6. Return Envelope(kind="rebalance.commit", payload={pid, epoch_to})

Draining node on commit receipt:
  7. Append (pid, epoch_to) to committed transfer log.
  8. Remove partition p from active ownership map.
```

Concurrent transfers bounded by `drain_max_concurrent` (default 4) via
`asyncio.Semaphore`. Transfer rate optionally throttled by
`drain_bandwidth_fraction` (default 1.0, i.e. unlimited).

### 4.3 Dual-ownership window

During the transfer of partition `p` (between source's step 1 and step 8):

- Writes for keys hashing to `p` are accepted by **both** the draining source
  and the target and cross-replicated via `replicate`.
- The draining source returns `TEMPORARY_UNAVAILABLE` for **primary**
  `kv.put` / `kv.delete` when it is acting as coordinator. This signals
  coordinators to route new writes to the next eligible member (which, after
  `DRAINING` gossip propagates, is the target node).
- The draining source continues to accept `replicate` envelopes for `p` so
  that writes that arrive via a lagging coordinator are still stored.

This ensures the target's committed partition is always a superset of what the
source had at the time of `rebalance.commit`.

### 4.4 `DRAINING → IDLE` transition

When all owned partitions are in the committed transfer log:

```
1. Broadcast leave notice to all known peers:
     Envelope(kind="node.leave", payload={node_id, epoch_to})

2. All peers compute ring_without_this_node independently:
     ring_epoch_to = epoch_to
     new_ring = ring.drop_nodes({node_id})

3. Acquire state file lock.
4. Write: phase = IDLE, ring_epoch = epoch_to, seq = seq + 1.
5. Release lock.

6. Emit Member gossip: phase=IDLE, seq=new_seq, generation=unchanged.

7. Stop serving kv.get (data is now with the targets).
8. KV and peer listeners remain bound; node is IDLE, awaiting next command.
```

Ring epoch advances exactly once per drain completion (not per partition).

### 4.5 Retry and deadline model

The drain operation uses the same two-axis budget as the join: `max_retries`
(hard attempt cap) and `deadline` (wall-clock budget). A failure is terminal if
either is exhausted first. `max_retries = -1` means unlimited within deadline.

**Backoff formula:** `wait = min(backoff_base × 2^attempt, backoff_max)`.

The deadline clock is **frozen while `PAUSED`** (see proposal 003 §3.5).
`deadline_remaining_s` and `retries_remaining` in `state.toml` record the
remaining budget at pause time and are restored on resume. Neither the deadline
nor the retry counter advances while the node is paused.

| Config key | Default | Meaning |
|------------|---------|---------|
| `[drain].max_retries` | `-1` | Max retry attempts; `-1` = unlimited within deadline |
| `[drain].attempt_timeout` | `30s` | Maximum time for a single transfer batch |
| `[drain].deadline` | `300s` | Total budget from `DRAINING` write |
| `[drain].backoff_base` | `5s` | Initial backoff interval |
| `[drain].backoff_max` | `60s` | Cap on exponential backoff |
| `[drain].max_concurrent` | `4` | Max concurrent partition transfers (`asyncio.Semaphore`) |
| `[drain].bandwidth_fraction` | `1.0` | Fraction of link bandwidth; 1.0 = unlimited |

On `max_retries` **or** deadline expiry:

```
1. Acquire state file lock.
2. Write: phase = FAILED, failed_from = "DRAINING".
3. Release lock.
4. Stop gossip emission.
5. Log: {event: "drain_failed", reason: "deadline_exceeded | max_retries_exceeded",
          committed: N, total: M}
6. Node enters inert mode.
```

Vnodes remain in the ring (epoch is unchanged) until the drain eventually
completes. Target nodes continue to accept reads for the uncommitted partitions
from the stalled source.

### 4.6 `FAILED → DRAINING` recovery

When `tourctl node leave` is issued on a `FAILED` node where
`failed_from = DRAINING`:

```
1. Acquire state file lock.
2. Verify failed_from = "DRAINING". If not, return error.
3. Write: phase = DRAINING, seq = seq + 1.
4. Release lock.
5. Emit gossip: phase=DRAINING, seq=new_seq, generation=unchanged.
6. Load committed transfer log.
7. Skip committed partitions.
8. Resume transfer for remaining partitions with a fresh deadline.
```

### 4.7 Restart from `DRAINING`

On restart with persisted `phase = DRAINING`:

```
1. Read state file → generation, seq, ring_epoch, last_known.
2. Emit gossip: phase=DRAINING, generation=unchanged, seq=seq+1.
   Peers re-learn the drain is in progress.
3. Load committed transfer log.
4. Re-contact peers using last_known seeds.
5. Resume transfers for uncommitted partitions.
6. Continue to DRAINING → IDLE as in §4.4.
```

Already-committed partitions are never re-transferred. The drain timeline
(including elapsed time before the crash) is not preserved: a fresh
`drain_deadline` begins from restart time.

### 4.8 Invariants

- Drain is **irreversible**. There is no `tourctl node cancel-leave`. A
  draining node must complete the drain before it can re-join.
- A draining node's vnodes remain in the ring throughout the entire drain. They
  are only removed when `DRAINING → IDLE` is committed.
- `DRAINING` nodes remain in the preference list for reads (`kv.get`). They
  return `TEMPORARY_UNAVAILABLE` for primary writes (`kv.put`, `kv.delete`) so
  coordinators fall through to the next eligible member.
- The committed transfer log is append-only per drain lifecycle. Starting a new
  drain (after `IDLE → JOINING → READY`) resets the log.
- Ring epoch advances exactly once per completed drain, not once per partition.

---

## Design decisions

### Decision: drain is irreversible

**Alternatives considered:** allow `tourctl node cancel-leave` to abort a drain
and return to `READY`.
**Chosen because:** a partially drained node holds an indeterminate ownership
state. Some partitions have been committed to the target and removed from the
source; the source's ring view and the target's ring view are diverging. There
is no safe, atomic undo operation for this. Forcing completion is the only
correct path. An operator who wants the node to re-join must complete the drain
first, wait for `IDLE`, then issue a new `tourctl node join`.

### Decision: DRAINING stays in the preference list

**Alternatives considered:** remove `DRAINING` from the preference list
immediately on the `READY → DRAINING` transition.
**Chosen because:** the draining node holds all its data for the entire drain
duration — excluding it from preference lists would drop a valid read replica
for minutes or hours. Additionally, the dual-ownership window requires the
draining node to receive replication writes for migrating partitions. Excluding
it from the preference list would break that protocol. The
`TEMPORARY_UNAVAILABLE` response on primary writes already ensures that
coordinators do not use a draining node as the write coordinator.

### Decision: source-initiated transfer

**Alternatives considered:** target-initiated pull (like the join protocol).
**Chosen because:** the draining node knows exactly which partitions it owns
and is motivated to drain them — it holds the transfer state and the deadline.
Source push lets the draining node control the pace, apply backpressure, and
enforce the `drain_max_concurrent` bound directly. The join protocol uses
target-pull because the joiner controls the join timeline; the drain protocol
uses source-push for the same reason.

### Decision: ring epoch advances only on drain completion

**Alternatives considered:** advance epoch on `READY → DRAINING` so peers
immediately start routing away from the draining node.
**Chosen because:** the draining node's vnodes must remain in the ring
throughout the drain for the dual-ownership protocol to function. Advancing the
epoch early would require peers to compute two ring states simultaneously
(epoch N with draining node, epoch N+1 without it) and decide which to use for
each request. A single epoch transition at completion is simpler and sufficient:
the `TEMPORARY_UNAVAILABLE` response on primary writes already achieves the
routing goal.

---

## Proposed code organisation

The following files do not yet exist.

```
tourillon/
  core/
    handlers/
      leave.py           # LeaveHandler: tourctl node leave request processing
      drain.py           # DrainCoordinator: drives the drain protocol loop
    ports/
      transfer.py        # TransferPort Protocol (rebalance.plan/transfer/commit)
    structure/
      drain.py           # DrainCheckpoint, CommittedTransferLog dataclasses

tourctl/
  core/
    commands/
      node_leave.py      # `tourctl node leave` CLI entry point
```

---

## Interfaces (informative)

```python
# tourillon/core/structure/drain.py

@dataclass
class CommittedTransferLog:
    """Append-only log of successfully committed partition transfers.

    Written alongside the node state file. Loaded on restart to skip
    already-committed partitions.
    """
    entries: list[tuple[int, int]]  # (pid, epoch_to)

    def is_committed(self, pid: int, epoch_to: int) -> bool: ...
    async def record(self, pid: int, epoch_to: int) -> None: ...


# tourillon/core/handlers/drain.py

class DrainCoordinator:
    """Drive the source-initiated drain protocol.

    Manages the transfer loop, retry/backoff logic, deadline enforcement,
    and the DRAINING → IDLE transition. Uses asyncio.Semaphore to bound
    concurrent transfers. All phase writes to the state file occur before
    any gossip record is emitted.
    """

    async def run(
        self,
        ring: Ring,
        owned_partitions: list[LogicalPartition],
        state: NodeStatePort,
        gossip: GossipPort,
        config: DrainConfig,
    ) -> None: ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | 3 in-memory nodes A, B, C all `READY`; `rf=3` | `tourctl node leave` on A | A: `DRAINING` → `IDLE`; B and C absorb A's partitions; ring epoch advances once |
| 2 | A `DRAINING` | `tourctl node join` | `INVALID_PHASE` |
| 3 | A `DRAINING` | Second `tourctl node leave` | `ALREADY_DRAINING` |
| 4 | A `DRAINING`, B `READY`; `rf=2` | `preference_list` for a token owned by A | A present in list (read eligible) |
| 5 | A `DRAINING` | `kv.get` sent direct to A | Accepted; returns value |
| 6 | A `DRAINING` | `kv.put` sent with A as primary coordinator | `TEMPORARY_UNAVAILABLE`; coordinator retries on B |
| 7 | A transfers P1–P60 then crashes | Restart A | P1–P60 not re-transferred; P61–P80 resumed; A reaches `IDLE` |
| 8 | A draining; `drain.deadline=5s`; B unreachable | `tourctl node leave` | A: `FAILED`; `failed_from=DRAINING`; epoch unchanged |
| 9 | A `FAILED` (`failed_from=DRAINING`); P1–P60 already committed | `tourctl node leave` | Resumes from P61; fresh deadline; A reaches `IDLE` |
| 10 | A `FAILED` (`failed_from=JOINING`) | `tourctl node leave` | Error: `failed_from=JOINING`; use `tourctl node join` |
| 11 | Write to partition P while P is being drained from A to B | `kv.put` | Write stored by both A (via replicate) and B (via coordinator); no write lost |
| 12 | Property (Hypothesis) | Any join/leave sequence on `bits=8` in-memory cluster | Final per-key HLC histories identical on all surviving nodes |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] 3 in-memory nodes: A leaves → B and C absorb partitions → A reaches `IDLE` → ring epoch advances once.
- [ ] Crash A mid-drain → restart → resumes without re-transferring committed partitions.
- [ ] Deadline expires → A `FAILED` → `tourctl node leave` → A resumes → A `IDLE`.
- [ ] `tourctl node join` on `DRAINING` returns `INVALID_PHASE`.
- [ ] `tourctl node pause` on `DRAINING` → `PAUSED`; `tourctl node resume` → `DRAINING` with frozen deadline; drain completes.
- [ ] Property-based tests: join/leave sequences on `bits=8` converge to identical ownership.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Pause/resume implementation details (specified in proposal 005), certificate
rotation, `tourctl ring inspect`.
