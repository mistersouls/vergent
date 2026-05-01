# Node Restart

This document is the authoritative reference for what happens when a Tourillon
node process is restarted after a crash, a clean shutdown, or a deliberate
`SIGTERM`. The restart behaviour is entirely determined by the **persisted
`MemberPhase`** present in local storage at the moment the process begins. No
network coordination is required to determine what phase to resume; the node
consults only its own durable state.

Cross-references:
- `docs/lifecycle/node-startup.md` — the startup sequence up to Step 4 (phase
  consultation)
- `docs/lifecycle/node-join.md` — the JOINING sequence that may be resumed
- `docs/lifecycle/node-drain.md` — the DRAINING sequence that may be resumed
- `docs/membership.md` — `generation`, `seq`, and the `Member` value object

---

## 1. The Phase-Persistence Guarantee

Tourillon writes the current `MemberPhase` to durable storage **before**
emitting any gossip that carries that phase. This ordering guarantee means
that, after a restart, the persisted phase is always at least as recent as
the last phase that any peer has observed via gossip. A node never resurrects
in a phase that is older than the phase its peers last saw.

In the in-memory adapter, "persistence" means the phase is recorded in the
in-process structure that survives within the lifetime of the test. For
production deployments backed by a durable engine, the write-before-gossip
ordering must be enforced by the storage adapter implementation.

---

## 2. Generation and Sequence Counters on Restart

Two counters in the `Member` value object govern how peers merge concurrent
gossip records:

- **`generation`** — incremented each time the node transitions from `IDLE`
  to `JOINING` (i.e., each time it re-enters the ring). It allows peers to
  discard gossip records from a previous incarnation of the same `node_id`.
- **`seq`** — incremented on every gossip emission within a generation. Peers
  use it to merge concurrent records about the same node within the same
  generation.

The critical invariant on restart is: **`generation` is incremented only when
the node transitions `IDLE → JOINING`, not on every restart**. A node that
restarts and resumes `READY` does not get a new `generation`; its peers
recognise the resumed identity as the same incarnation and update their gossip
table using `seq`. A new `generation` would falsely imply the node had left and
rejoined the ring.

---

## 3. Restart from IDLE

A node that was in `IDLE` when it stopped is almost indistinguishable from a
fresh start. The restart sequence is:

1. Config is loaded and validated (Steps 1–2 of the startup sequence).
2. Storage is initialised; phase `IDLE` is read back from storage.
3. TCP listeners are bound.
4. Phase `IDLE` is persisted again (idempotent; no observable change).
5. The node waits for an explicit `join` command, as on a fresh start.

No gossip is emitted. No seeds are contacted. The operator must issue a `join`
command (or re-configure seeds and re-issue `tourillon node join`) to bring
this node back into the cluster.

---

## 4. Restart from JOINING

A node that crashed or was shut down while in `JOINING` resumes the join
protocol from the beginning of the seeded join sequence (§4 of
`docs/lifecycle/node-join.md`). It does **not** reset to `IDLE`. The rationale
is that an operator explicitly issued `join` before the crash; the system
should carry that intent forward without requiring operator intervention again.

The restart sequence:

1. Config is loaded and validated.
2. Storage is initialised; phase `JOINING` is read back along with the
   persisted `generation` and `seq`.
3. TCP listeners are bound.
4. The bootstrap layer reads the seeds from config (or the persisted seed
   list, if contacts were saved before the crash) and re-initiates seed
   contact as in `docs/lifecycle/node-join.md §4.1`.
5. Because `generation` was already incremented when `IDLE → JOINING`
   happened the first time, it is **not** incremented again on this restart.
   The node re-enters the ring under the same generation as before the crash.
6. Partition transfers already confirmed by a `rebalance.commit` before the
   crash are treated as complete and are not re-transferred. The node resumes
   only the in-progress transfers from their last durable checkpoint.
7. Once all primary partition slots are committed the node transitions to
   `READY` as described in `docs/lifecycle/node-join.md §4.6`.

If the joining node's vnodes were already present in the ring epoch at the
time of the crash, they remain in the ring. The restart does not remove them
and re-add them; it picks up where the transfer protocol left off. If the ring
epoch has since advanced (e.g., another node joined or left while this node
was down), the joiner recomputes the delta against the latest epoch before
resuming transfers.

**Observable impact on peers:** peers that received the original `JOINING`
gossip record before the crash will not observe any new gossip until the
restarted node re-connects to seeds. At that point the restarted node emits a
gossip record with the same `generation` but a higher `seq`, confirming it is
alive.

---

## 5. Restart from READY

A node that was in `READY` when it stopped resumes `READY` immediately. No
ring negotiation is required before the listeners open; the node already has
a valid ring view in storage. The restart sequence:

1. Config is loaded and validated.
2. Storage is initialised; phase `READY` is read back along with the
   persisted `generation` and `seq`.
3. TLS contexts are constructed and TCP listeners are bound.
4. The ring view is loaded from storage. Partition ownership is computed from
   the persisted ring epoch. The node can immediately answer routing questions.
5. Phase `READY` is persisted with `seq` incremented by 1. This bump
   distinguishes the post-restart gossip record from the last pre-crash record
   and signals liveness to peers.
6. The gossip engine emits a `READY` record to seeds. Peers merge the new
   `seq`-incremented record into their gossip tables; any records from the
   pre-crash incarnation with lower `seq` are superseded.
7. The node begins accepting KV traffic on `servers.kv` and replication /
   gossip traffic on `servers.peer`.

Because `READY` restarts do not advance the ring epoch, no rebalance plan is
triggered. Peers that received write traffic destined for this node while it was
down will have buffered those writes in their hinted handoff queues. Once the
hinted handoff protocol detects that the node is `READY` again, any buffered
hints are replayed to the node in HLC order.

**On ring view staleness:** the persisted ring view may be slightly behind the
current epoch if other membership events happened while this node was down. The
gossip engine reconciles this by merging the epoch digests received from seeds
during the re-announcement phase. If the persisted epoch is stale the node
updates its ring view before serving writes, ensuring it computes preference
lists from the current topology.

---

## 6. Restart from DRAINING

A node that was in `DRAINING` when it stopped resumes the drain protocol. A
drain is irreversible; there is no mechanism for a restarted draining node to
cancel the drain and re-enter `READY`. The restart sequence:

1. Config is loaded and validated.
2. Storage is initialised; phase `DRAINING` is read back along with the
   drain checkpoint: the set of partitions that have already issued
   `rebalance.commit` and the set of partitions still in flight.
3. TCP listeners are bound.
4. The node reconnects to the ring and emits gossip with `phase = DRAINING`.
   This re-announces its presence so that coordinators routing writes to its
   partitions resume the dual-ownership fan-out.
5. For each partition that completed before the crash (has a stored commit
   ack), the node treats the ownership transfer as final. It does not
   re-transfer these partitions.
6. For each partition still in flight at the time of the crash, the node
   re-initiates the transfer from its last checkpoint. If the target reports
   the partition as already committed, the node accepts that and moves on.
7. Once all partitions have transferred and all in-flight `replicate` traffic
   has settled, the drain completes normally and the node transitions to `IDLE`
   as described in `docs/lifecycle/node-drain.md`.

**Impact of a new ring epoch during the crash window:** if the ring epoch
advanced while the draining node was down (e.g., another node joined),
the restarted draining node recomputes its drain plan against the new epoch
and re-derives the correct target nodes. Partitions that were not affected by
the interleaved topology change resume transferring to their original targets.

---

## 7. Generation Counter Summary

| Restart scenario | `generation` | `seq` on restart | Rationale |
|-----------------|:------------:|:----------------:|-----------|
| Restart from IDLE | Unchanged | Unchanged (IDLE emits no gossip) | No ring re-entry occurred |
| Restart from JOINING (same join attempt) | Unchanged | Incremented by 1 | Same incarnation, re-announcing liveness |
| IDLE → JOINING (join issued) | **+1** | Reset to 0 | New incarnation begins |
| Restart from READY | Unchanged | Incremented by 1 | Same incarnation, re-announcing liveness |
| Restart from DRAINING | Unchanged | Incremented by 1 | Same incarnation, drain resumes |

The `generation` counter is stored durably. It must survive restarts unchanged.
Losing the `generation` value — for example by wiping local storage — causes
peers to momentarily accept stale gossip records until the generation
mismatch is detected and corrected by the merge rule. Operators should treat
the local storage content as identity state, not as pure data cache.

---

## 8. Summary Decision Table

| Persisted phase | What the node does on restart |
|----------------|------------------------------|
| Not present (first start) | Enters `IDLE`, waits for `join` |
| `IDLE` | Enters `IDLE`, waits for `join` |
| `JOINING` | Re-contacts seeds, resumes partition handoffs, transitions to `READY` when done |
| `READY` | Immediately serves traffic, re-announces to seeds via gossip |
| `DRAINING` | Resumes drain from checkpoint, transitions to `IDLE` when complete |
