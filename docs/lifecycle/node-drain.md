# Node Drain

This document describes the complete sequence that begins when an operator
issues a `leave` command to a Tourillon node that is in the `READY` phase and
ends when the node transitions to `IDLE` and broadcasts its departure from the
ring. It also covers the irreversibility of a drain, the crash-and-resume path,
and all error conditions.

Cross-references:
- `docs/lifecycle/node-restart.md §6` — restarting from `DRAINING`
- `docs/membership.md` — `MemberPhase` FSM and eligibility rules during drain
- `docs/rebalance.md` — the partition transfer protocol (`rebalance.plan`,
  `rebalance.transfer`, `rebalance.commit`)
- `docs/ring.md §5.2` — why `DRAINING` nodes remain in preference lists

---

## 1. Trigger

A drain is initiated by the `leave` command, issued via `tourctl`:

```
tourctl node leave
```

The command is valid only when the node is in `READY` phase. Issuing it while
the node is `IDLE`, `JOINING`, or already `DRAINING` produces an error with no
state change.

Draining is **irreversible for the current lifecycle cycle**. Once a node
enters `DRAINING`, the only valid next state is `IDLE`. A node that changes its
mind after issuing `leave` must complete the drain, reach `IDLE`, and then
issue a new `join` command to re-enter the cluster.

---

## 2. READY → DRAINING Transition

When the `leave` command is received, the node performs these operations before
emitting any gossip:

1. Persists `phase = DRAINING` and increments `seq` in local storage. The
   phase is written before gossip to guarantee that a crash immediately after
   this point is recovered as a `DRAINING` restart.
2. Emits a gossip record with `phase = DRAINING` to all known peers. This
   signals to the rest of the cluster that a new ring epoch will soon be
   published in which this node's vnodes are still present but the node is
   vacating.

Note that `READY → DRAINING` does **not** produce a new ring epoch. The
draining node's vnodes remain in the ring throughout the entire drain window.
A new epoch is published only when the drain completes and the node's vnodes
are removed (see §6). This asymmetry is documented in `docs/membership.md §6`.

---

## 3. What a Node in DRAINING Can and Cannot Do

The draining node remains a valid member of the ring's preference lists for
reads and for the dual-ownership write fan-out. This is essential: excluding
it would silently remove a valid read replica and break the handover protocol.

| Operation | DRAINING behaviour |
|-----------|-------------------|
| Serve `get` for owned partition | Yes — fully valid |
| Accept `put` / `delete` as **primary** for owned partition | No — returns `TEMPORARY_UNAVAILABLE` so coordinators fall through to the next owner |
| Accept `put` / `delete` as **secondary** in fan-out | Yes — required for the dual-ownership window |
| Accept `replicate` traffic | Yes |
| Act as transfer source for `rebalance.transfer` | Yes — this is its primary job |
| Accept a `leave` command | No — already draining |
| Accept a `join` command | No — not in IDLE |

---

## 4. Partition Transfer Sequence

The drain proceeds partition-by-partition. Because every node independently
computes the ownership delta as a pure function of the ring state, no
coordination between draining node and targets is required to agree on which
partitions are migrating.

### Step 1 — Plan derivation

The draining node derives the rebalance plan for each partition it owns. The
plan identifies the target node(s) that will absorb the partition at the next
ring epoch (`E_new = E_current + 1`, the epoch where this node's vnodes are
absent). The plan is a pure function of `(E_current, E_current + 1, ring)`.

### Step 2 — Transfer initiation

For each partition, the draining node sends a `rebalance.plan` envelope to the
designated target, advertising the partition range, the ring epoch context, and
the expected integrity metadata (record count, high-watermark HLC, and digest
seed). The target validates the plan against its own ring view and responds with
acceptance or a `CONFLICT_OR_ORDERING_ERROR` if its epoch view differs.

### Step 3 — Streaming transfer

The draining node streams `Version` and `Tombstone` records to the target via
`rebalance.transfer` envelopes. Records are emitted in HLC order per key and in
any order across distinct keys. The target stores incoming records in a
quarantined staging area, invisible to reads until commit.

Any write arriving at the draining node for a migrating partition during
transfer is accepted (as a secondary fan-out recipient) and immediately
replicated to the target through the standard `replicate` path so that no
acknowledged write is lost.

### Step 4 — Integrity validation and commit

Once all records for a partition have been streamed, the target validates:

- A digest over the ordered set of `(StoreKey, hlc, kind, payload_hash)` tuples.
- Record count and high-watermark HLC compared against the values declared in
  the `rebalance.plan`.

If validation passes, the target issues a `rebalance.commit` and promotes the
partition from quarantine to its live read path. The draining node receives the
commit, marks the partition as transferred in its drain checkpoint, and
relinquishes primary ownership.

### Step 5 — Concurrent transfers

Transfers for multiple partitions proceed concurrently, bounded by an
`asyncio.Semaphore` to prevent the drain from saturating the node's outbound
bandwidth. The semaphore limit is configurable.

---

## 5. Drain Completion

The drain is complete when every partition for which the draining node is a
source has produced a valid `rebalance.commit` from its target. At that point:

1. All in-flight `replicate` traffic targeting the draining node settles (any
   new writes from this point forward are routed directly to the new owners).
2. Any hinted handoff entries buffered on behalf of peers are forwarded to
   their intended destinations.
3. The draining node broadcasts a **leave notice** to the ring, which causes
   every peer to publish a new ring epoch `E_leave` from which this node's
   vnodes are absent.
4. The draining node persists `phase = IDLE` and emits a final gossip record
   with `phase = IDLE`.
5. The node is now fully drained. It continues to serve health probes but
   rejects all data-plane traffic with `TEMPORARY_UNAVAILABLE`.

---

## 6. Ring Epoch on Drain Completion

The departure triggers ring epoch `E_leave = E_prior + 1`. Every peer that
receives the leave notice independently derives the new ring topology and
computes any additional rebalance plans needed to restore the replication
factor on partitions that lost a replica when this node departed.

This is symmetric with the join epoch transition: a join adds vnodes and
advances the epoch; a completed drain removes vnodes and advances the epoch.
The intermediate drain period (from `READY → DRAINING` through the final leave
notice) does not produce its own epoch change, keeping epoch churn low during
lengthy transfers.

---

## 7. Crash and Resume

If the draining node crashes mid-drain, peers continue operating normally.
Partitions that had already committed before the crash are handled by the new
owners. Partitions still in flight are treated as uncommitted by the targets
(they do not serve them from quarantine) and by the source (the drain
checkpoint still lists them as in-progress).

When the draining node restarts (see `docs/lifecycle/node-restart.md §6`):

- It re-reads the drain checkpoint from storage.
- Partitions already committed: accepted as complete; not re-transferred.
- Partitions still in-flight: transfer resumes from the last per-batch HLC
  checkpoint.
- If the target crashed too and lost its quarantined state, it replies to the
  resumed `rebalance.plan` as though it has received nothing; the source
  streams from the beginning for that partition.

---

## 8. Error and Abort Paths

| Condition | Behaviour |
|-----------|-----------|
| Target unreachable | Retry with backoff; drain paused for that partition; other partitions continue |
| Target rejects plan (epoch mismatch) | Source re-derives plan against latest epoch; retry |
| Integrity validation fails at target | Target discards in-progress transfer; source retries from last checkpoint |
| Crash mid-transfer | Resume from checkpoint on restart; see §7 |
| New ring epoch supersedes drain plan | Source re-derives plan for affected partitions; stable partitions unaffected |
| `leave` command issued a second time | `ALREADY_DRAINING` error; no state change |
| `join` command issued during drain | `INVALID_PHASE` error; drain must complete first |

---

## 9. Observable Signals During Drain

An operator monitoring a drain can observe the following:

- The draining node's gossip record carries `phase = DRAINING`.
- The `tourctl ring inspect` command shows the partition migration progress
  (queued, in-flight, and committed partition counts).
- Structured logs emit plan derivation, transfer batch progress, and commit
  events as specified in `docs/rebalance.md — Observability`.
- Client writes routed to the draining node as primary receive
  `TEMPORARY_UNAVAILABLE` and are retried by coordinators against the next
  preference-list member; clients observe no write failures during a healthy
  drain.
- Once the leave notice propagates, the draining node disappears from all
  preference lists and `phase = IDLE` gossip confirms the departure.
