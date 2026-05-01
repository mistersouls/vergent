# Convergence

This document defines the convergence model for Tourillon: how per-key update
ordering is established, how concurrent writes are represented without
information loss, how replicas apply updates monotonically, and how hinted
handoff preserves ordering through temporary unavailability. It is the
normative reference for any component that reads, writes, or merges version
records.

---

## 1. Hybrid Logical Clock Ordering

Every update in Tourillon — whether a write or a delete — carries a
**Hybrid Logical Clock (HLC)** timestamp as its ordering metadata. An HLC
combines a wall-clock component with a logical component so that:

- Two events on the same node are always ordered, even if the wall clock does
  not advance (the logical component is incremented).
- An event that causally follows another event always has a higher HLC value,
  regardless of clock skew between nodes.
- HLC values are compact (a `(wall_ms, seq, node_id)` triple) and can be
  compared without global coordination.

The HLC is the **only** source of truth for per-key ordering. Wall-clock
`time.time()` is never used to order updates. No random tiebreaks exist.
When two updates have identical `(wall_ms, seq)` — an extremely rare but
possible condition — the `node_id` string provides the deterministic
tiebreak. The same tiebreak rule must be applied identically on every node.

Every update MUST carry HLC metadata that is stable across retries and
replication. Retransmitting a write to a replica must not alter the HLC of the
original write. Replaying a log entry must produce the same HLC, same value,
and same version identity as the original application.

---

## 2. Append-Only Version Stream

The state of a key is not a single value: it is an ordered, append-only stream
of `Version` and `Tombstone` records addressed by `StoreKey`. Each record is
immutable once written; no record is ever modified or removed by normal
operation.

The append-only invariant serves two purposes. First, it makes log replay
deterministic: replaying the same sequence of records onto an empty storage
always produces the same final stream. Second, it makes concurrent write
detection straightforward: if two records at the same HLC level target the
same key, both are retained in the stream.

### Live values and tombstones

A `Version` record carries `(StoreKey, hlc, value)`. A `Tombstone` record
carries `(StoreKey, hlc)` with no value payload. Tombstones are first-class
citizens of the stream: a tombstone is not a deletion of the previous version
but a new record asserted at a higher HLC that signals the absence of a value
at that point in time. A `Version` written after a `Tombstone` at a higher HLC
revives the key.

### Visible state

The visible state for a `get` request is the set of records at the highest HLC
level in the stream for the queried `StoreKey`. If that highest level contains
a single `Version`, the visible value is that version's payload. If it contains
multiple `Version` records at the same HLC (concurrent writes), all of them are
returned. If it contains only a `Tombstone`, the key is reported as absent.

Returning all concurrent versions rather than silently picking one is correct
because Tourillon makes no assumptions about value semantics. The application
layer is responsible for merging concurrent versions if needed. Losing a
concurrent write by silently discarding it would violate the append-only
invariant.

---

## 3. Conflict Model

A conflict, in Tourillon's model, is not an error: it is the normal outcome of
two clients writing to the same key faster than replication can propagate the
writes. The system does not attempt to prevent conflicts by serialising writes
through a coordinator; instead, all concurrent versions are preserved and
surfaced to the reader.

Conflict resolution, when needed, is the application's responsibility. Tourillon
provides the complete, ordered version stream for any key so that the
application can apply its own merge logic (last-writer-wins, vector-clock
comparison, domain-specific reconciliation).

The HLC ordering rule guarantees that, for any pair of non-concurrent versions
on the same key (where one causally precedes the other), the causal order is
correctly reflected in the HLC order. A replica that receives the versions
out of order will still converge to the correct stream once both are applied.

---

## 4. Replica Apply Rules

The replica apply path is the code that integrates an incoming `Version` or
`Tombstone` into local storage. It must satisfy two invariants:

**Monotonicity.** Applying a record to storage must never decrease the HLC
high-watermark for the affected key. The record is appended to the stream if
it is not already present; existing records with lower or equal HLC are not
modified.

**Idempotency.** Applying the same record twice must leave storage in the same
state as applying it once. The apply logic checks whether the
`(StoreKey, hlc, kind)` triple already exists before inserting. If it exists,
the duplicate is silently discarded.

These two invariants together guarantee correct behavior under out-of-order
delivery and duplicate delivery, both of which are expected in a distributed
system with retries and gossip.

### Out-of-order delivery

A replica may receive V2 before V1 for the same key. This is acceptable: the
apply path appends records to the stream in the order they arrive, and the
stream is subsequently traversed in HLC order when answering a `get` request.
The final visible state is identical regardless of the delivery order.

---

## 5. Hinted Handoff Ordering Guarantees

When a replica is temporarily unreachable, writes that would have gone to it
are buffered as hints on the coordinator node. The hint stores the original
`(StoreKey, hlc, value)` tuple unchanged. The HLC is never modified when a
write is converted into a hint; if it were, the hint would arrive at the
recovering replica with a different ordering metadata than the write had when
it was acknowledged, breaking the convergence guarantee.

When the hinted handoff target recovers and is re-classified as reachable, the
coordinator delivers buffered hints in HLC order per key. Delivering hints out
of HLC order would force the target to buffer and re-sort, adding complexity
without benefit. The in-memory hinted handoff queue maintains a per-key
sorted structure keyed by HLC so that delivery can always proceed in order.

Hints that remain undelivered for longer than a configurable TTL are expired.
Expiry is observable via structured log events and metrics (hint age histogram,
expired hint count). Expired hints represent a potential data gap at the
recovering node; the anti-entropy or read-repair paths (future milestone) are
responsible for detecting and closing such gaps.

---

## 6. Convergence Guarantee

Given the properties above, Tourillon guarantees **deterministic eventual
convergence**: for any two replicas that have received the same set of
`Version` and `Tombstone` records for a given `StoreKey`, they will produce
identical visible state when answering a `get` query for that key, regardless
of the order in which the records were received.

This guarantee holds as long as:

- The HLC tiebreak rule (`node_id` string comparison) is applied identically
  on every node.
- No record is modified after it is written (append-only invariant is
  preserved).
- Idempotent apply is correctly implemented (duplicates are silently dropped).
- The network eventually heals and all writes are eventually delivered (either
  directly or via hinted handoff).

Long-lived network partitions delay convergence but do not produce
contradictory final states. Two replicas that are partitioned from each other
and receive concurrent writes will both accumulate the writes received on their
side. Once the partition heals and the versions are cross-replicated, both
replicas arrive at the same version stream.
