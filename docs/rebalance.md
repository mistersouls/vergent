# Rebalance

## Functional Intent

Rebalance is the process by which Tourillon redistributes partition ownership
and data when the consistent-hashing ring changes. Because Tourillon is
leaderless, no single node decides when or how to rebalance: every node
independently derives the same plan from the same ring epoch and the same
deterministic ownership function. Rebalance MUST preserve the convergence
guarantees defined in `docs/convergence.md` (per-key HLC ordering,
append-only version history, idempotent replay) and the security guarantees
defined in `docs/security.md` (mandatory mTLS on every transfer). It MUST be
safe under continuous churn, gossip lag, asymmetric reachability, and crashes
at any step of the protocol — including the worst case where multiple
concurrent topology events partially overlap.

This document is the normative reference for the design and behavior of the
rebalance subsystem. It complements:

- `docs/architecture.md` — *Rebalance Model* (high-level invariants).
- `docs/protocol.md` — `rebalance.plan`, `rebalance.transfer`,
  `rebalance.commit` envelope kinds.
- `docs/operations.md` — *Bootstrap, Join, Leave Procedures* and
  *Continuous Churn and Unstable Network Assumptions*.

## Goals

- Keep ownership transfer **deterministic** for a given ring epoch so that
  any two nodes compute identical source/target sets without coordination.
- Move **only** the partitions affected by an ownership delta. Stable
  partitions MUST NOT be touched.
- Preserve **per-key HLC ordering** and the **append-only** version history
  of every transferred key, including tombstones.
- Remain **safe under churn**: join, leave, drain, crash, and gossip lag may
  all occur concurrently without producing inconsistent ownership or data
  loss.
- Keep foreground read/write traffic available on non-migrating partitions
  and best-effort available on migrating partitions through dual-ownership
  windows.
- Be **observable** at every step: queued, in-flight, transferred bytes,
  lag, and outcome of integrity validation.
- Be **resumable** and **idempotent**: any step interrupted by a crash or
  network failure MUST be safe to retry without violating convergence.

## Terminology

- **Ring epoch (`E`)** — monotonically increasing version number of the
  ring. Every membership change produces a new epoch. Stale epochs are
  rejected on receipt (`docs/roadmap.md` Phase 2a).
- **Ownership delta (`Δ(E_old → E_new)`)** — the set of
  `(partition, replica_set_old, replica_set_new)` triples that differ
  between two epochs.
- **Source** — a node that owned a partition at `E_old` but does not at
  `E_new` for the same replica slot, OR a node that retains ownership and is
  acting as a transfer source for a new replica.
- **Target** — a node that gains ownership of a partition at `E_new`.
- **Drain** — operator-initiated graceful leave that completes all outbound
  transfers before the node exits the ring.
- **Dual-ownership window** — the interval during which both the previous
  and the new owners answer reads and accept writes for a migrating
  partition, ensuring no availability gap.

## Normative Requirements

### Ring epoch and plan derivation

- A node MUST compute a rebalance plan strictly as a pure function of the
  pair `(E_old, E_new)` and the consistent-hash ring state at those epochs.
  The plan MUST NOT depend on wall-clock time, random tiebreaks, or local
  process state.
- The plan MUST enumerate, for each affected partition, the deterministic
  ordered list of `(source, target)` pairs derived from the replica-set
  ordering rule defined in `docs/architecture.md`.
- A node MUST refuse to act on a plan whose `E_new` it has not yet observed
  through gossip, and MUST refuse to act on a plan whose `E_old` is older
  than the latest epoch it has already committed locally.
- When two epoch transitions arrive close in time (`E → E+1 → E+2`), a node
  MAY fold them into a single plan from `E` to `E+2` provided the resulting
  plan is identical to the deterministic composition of `Δ(E → E+1)` and
  `Δ(E+1 → E+2)`. Folding MUST NOT alter per-key ordering metadata.

### Plan, transfer, and commit protocol

The rebalance protocol uses three envelope kinds defined in
`docs/protocol.md`. Each step is independent, idempotent, and resumable.

1. **`rebalance.plan`** — the source advertises to the target the list of
   partitions, key ranges or partition tokens, and ring epoch context that
   it intends to transfer. The target MUST validate that the plan is
   consistent with its own view of `E_new`; if not, it MUST reject the plan
   with a `CONFLICT_OR_ORDERING_ERROR` and the source MUST re-derive against
   the latest gossiped epoch.
2. **`rebalance.transfer`** — the source streams the `Version` and
   `Tombstone` records for the announced partitions. Each record MUST carry
   its original HLC ordering metadata unchanged. Records MUST be applied at
   the target through the same idempotent apply path as `replicate`, so
   that a partial transfer followed by a retry converges to the same state.
3. **`rebalance.commit`** — once the target has received and validated the
   transfer, it returns a commit acknowledgement. The source MUST NOT
   relinquish ownership until it has received a valid `rebalance.commit`
   for that partition at `E_new`. The target MUST NOT advertise itself as
   the authoritative owner until it has issued the commit.

### Integrity validation

- Before issuing `rebalance.commit`, the target MUST validate the integrity
  of the received partition. Validation MUST include, at minimum:
  - A digest over the ordered set of `(StoreKey, hlc, kind, payload_hash)`
    tuples covering all transferred records.
  - A count and a high-watermark HLC, compared against the values declared
    by the source in the `rebalance.plan` envelope.
- If integrity validation fails, the target MUST discard the in-progress
  transfer state for that partition and MUST NOT issue `rebalance.commit`.
  The source MUST retain ownership and MAY retry the transfer with bounded
  exponential backoff.

### Per-key ordering preservation

- Transfer order between source and target MUST NOT alter per-key HLC
  ordering. The source MAY stream records in any order across distinct
  keys, but for any single `StoreKey` it MUST emit versions in HLC order
  to bound out-of-order buffering at the target.
- The target MUST apply received records through the standard append-only
  apply path and MUST NOT collapse, reorder, or rewrite HLC metadata.
- Tombstones MUST be transferred with the same fidelity as live versions.

### Dual-ownership window and write path

- During the interval between `rebalance.plan` and `rebalance.commit`, both
  the source and the target MUST accept writes for the migrating partition.
  Writes received by either node MUST be replicated to the other side
  through the standard `replicate` flow so that no acknowledged write is
  lost across the handover.
- A coordinator that receives a client write for a migrating partition
  MUST fan out to **both** the previous and the new replica set members,
  and MUST count an acknowledgement from either side as valid for quorum
  purposes, provided the union of acknowledgers covers the configured
  write consistency level.
- A coordinator that receives a client read for a migrating partition MUST
  query the union of previous and new owners and merge the results
  through the standard HLC ordering rule. Stale reads MUST NOT occur as a
  result of the handover.

### Rate limiting and backpressure

- Rebalance traffic MUST be rate-limited per source node so that
  foreground read/write SLOs are preserved. The limit MUST be configurable
  and SHOULD default to a value that uses no more than a documented
  fraction of the node's outbound bandwidth.
- Concurrent in-flight transfers per node MUST be bounded by an
  `asyncio.Semaphore` to prevent task accumulation under bursty plans.
- When the target signals backpressure (full write buffer, transport
  flow control, or storage write queue saturation), the source MUST
  suspend streaming through the `asyncio.Event` flow-control helper
  defined in `docs/protocol.md` and MUST resume only when the target
  signals readiness.

### Idempotent replay and crash safety

- Every step of the rebalance protocol MUST be replayable. A source that
  crashes mid-transfer MUST resume from the last durable checkpoint after
  restart; a target that crashes mid-receipt MUST be able to discard any
  partially received batch and request retransmission.
- The local log MUST record:
  - Plan acceptance (source and target sides) keyed by
    `(partition, E_new)`.
  - Per-batch transfer progress (last HLC applied at the target).
  - Commit issuance (target) and commit acknowledgement (source).
- Replay of any of these log entries MUST NOT cause duplicate apply,
  duplicate ownership change, or duplicate commit.

## Operational Triggers

### Join

When a new node joins, gossip propagates a new ring epoch in which the
incoming node owns a deterministic subset of partitions previously held by
its neighbors. Each affected source independently derives the same plan
and begins streaming to the new owner. Joins MUST:

- Be admitted in controlled batches when many nodes join together; the
  membership layer SHOULD coalesce simultaneous joins into a small number
  of epoch transitions to bound plan churn.
- Refuse to serve writes from the joining node until it has completed the
  transfer and committed for at least the partitions in its primary
  replica slots.
- Continue to serve reads from previous owners until the dual-ownership
  window closes via `rebalance.commit`.

### Graceful leave (drain)

A drain is an operator-initiated graceful leave triggered by `tourctl`
(see `docs/operations.md`). The draining node MUST:

- Remain reachable on its `servers.peer` listener for the entire drain.
- Reject new writes routed to it as a primary owner with
  `TEMPORARY_UNAVAILABLE` so coordinators retry on the new owner.
- Continue to act as a source for outbound transfers and to acknowledge
  in-flight `replicate` traffic.
- Exit the ring only after every partition for which it is a source has
  produced a valid `rebalance.commit` from the corresponding target.
- If the drain is cancelled mid-flight, the node MUST re-enter the ring
  cleanly without losing committed transfers.

### Ungraceful leave and crash

When a node disappears without draining, failure detection (heartbeat +
suspicion model from `docs/roadmap.md` Phase 2a) eventually demotes it.
The new ring epoch reassigns its partitions to surviving replicas. The
system MUST:

- Treat the missing node's data as recoverable from the remaining replicas
  in its replica set; rebalance plans are derived from the surviving
  replicas only.
- Use hinted handoff (`docs/convergence.md` — *Hinted Handoff Ordering
  Guarantees*) to absorb writes targeted at the dead node until it is
  formally removed or rejoins.
- If a recovered node returns under the same identity at a later epoch, it
  MUST synchronise to the latest epoch, replay missed writes from hints,
  and only then re-enter the ownership computation.
- Never assume that a node declared dead is permanently gone. A returning
  node at `E_old` MUST be reconciled, not discarded.

### Drain followed by crash

If a node crashes during a drain, surviving replicas MUST treat any
already-committed transfers as final and MUST initiate fresh plans for the
partitions whose drain transfer had not yet committed, using the
post-failure ring epoch. Pending hints destined to the crashed node MUST
either be re-routed to the new owner or discarded if the new owner is
already up-to-date through replication.

## Worst-Case Scenarios

### Concurrent join and leave

A node joins while another leaves at nearly the same wall-clock time.
Gossip MAY deliver the two epoch transitions in different orders to
different nodes. Because plans are pure functions of the pair
`(E_old, E_new)`, every node eventually computes the same composed plan
once it observes both transitions. Nodes that observed only one transition
MUST act on that transition only; their work is not wasted because every
intermediate plan is a deterministic prefix of the composed plan.

### Gossip lag and partial views

Some nodes may temporarily disagree on the current epoch. The protocol
defends against this by:

- Requiring `E_old` and `E_new` to be carried in every `rebalance.plan`
  and validated by the target.
- Refusing to apply a plan whose epochs are unknown locally; the target
  responds with `CONFLICT_OR_ORDERING_ERROR` and the source retries after
  a short delay.
- Using gossip ring epoch as the only source of truth for membership
  decisions; per-message timestamps MUST NOT influence the plan.

### Network partition during rebalance

If the source and target become unreachable mid-transfer:

- The source MUST retain ownership and MUST NOT mark the partition as
  transferred.
- The target MUST keep partial state in a quarantined area; it MUST NOT
  serve reads for the migrating partition until commit.
- On heal, the source MUST resume from the last durable checkpoint and the
  target MUST request the next batch starting at its last applied HLC.
- If the partition lasts long enough that a new ring epoch supersedes the
  in-flight plan, the source MUST abort the transfer and recompute against
  the new epoch.

### Rebalance storm

A burst of concurrent topology events (mass join, mass leave, VM reboot
wave) MAY produce many overlapping plans. The system MUST defend by:

- Coalescing epoch transitions in the membership layer when possible.
- Bounding concurrent in-flight transfers per node via the semaphore.
- Surfacing storm conditions via the *rebalance storm* runbook in
  `docs/operations.md` so operators can reduce churn or raise rate limits.
- Never abandoning the deterministic plan derivation under load: the
  number of in-flight transfers MAY drop, but the eventual plan MUST be
  the same plan that a quiescent cluster would derive.

### Flapping membership

A node that repeatedly suspects/unsuspects another (partition flapping)
MUST NOT trigger an unbounded rebalance loop. The membership layer SHOULD
apply a hysteresis window before promoting a suspicion into a new epoch,
and MUST never produce an epoch transition for a node that returns to
healthy state within the suspicion timeout.

### Crash mid-commit

If the source crashes after the target has issued `rebalance.commit` but
before the source has durably recorded the commit acknowledgement:

- On restart, the source MUST consult the target for the commit status of
  every partition still listed as in-flight in its local log.
- If the target reports the partition as committed, the source MUST drop
  ownership without resending data.
- If the target reports it as not committed, the source MUST resume from
  its last checkpoint.

### Split-brain ownership claim

Two nodes MUST never both claim authoritative ownership of the same
replica slot of the same partition at the same ring epoch. The protocol
guarantees this because:

- Ownership is a deterministic function of `E`.
- A target advertises authoritative ownership only after issuing a commit.
- A source relinquishes ownership only after seeing a commit ack.
- Reads during the dual-ownership window union both sides without picking
  a single winner, so a transient overlap does not cause divergence.

### Clock skew and HLC drift

HLC ordering metadata is the only source of truth for per-key ordering.
Rebalance MUST NOT compare wall-clock timestamps. Bounded clock skew
between source and target therefore cannot produce a divergent transfer
outcome. Unbounded skew is a separate operational concern handled in
`docs/operations.md`.

## Observability

A rebalance run MUST emit structured log events covering:

- Plan derivation: `(E_old, E_new, partitions, source, target)`.
- Transfer batches: `(partition, batch_id, records, bytes, last_hlc)`.
- Commit decisions: `(partition, target, decision, integrity_digest)`.
- Aborts and retries with explicit reason codes drawn from the protocol
  error model (`docs/protocol.md`).

Metrics MUST include:

- Partitions queued, in flight, completed, failed.
- Bytes and records transferred, both per source and per target.
- Time to commit per partition, p50/p95/p99.
- Number of plans aborted due to epoch supersession.
- Hinted handoff backlog created or drained as a side effect.

## Testing Requirements

`docs/testing.md` defines the general scale and fault test surface. The
rebalance subsystem MUST additionally be covered by:

- **Determinism tests** — given two ring epochs, every node derives the
  same plan, regardless of arrival order in gossip.
- **Idempotency tests** — a transfer interrupted at every step (plan,
  partial transfer, integrity failure, commit, commit ack) recovers to the
  same final state on retry.
- **Dual-ownership tests** — writes received during the handover window
  are visible from both sides, and quorum semantics are preserved.
- **Concurrent topology tests** — overlapping join/leave/drain/crash
  scenarios produce a final ring state and data state identical to the
  serial composition of the underlying plans.
- **Gossip lag fault injection** — delay or reorder gossip and verify
  that no node commits a transfer based on a stale epoch.
- **Partition mid-transfer** — induce a network partition between source
  and target during transfer and verify resume-from-checkpoint behavior.
- **Storm tests** — drive simultaneous joins and leaves under sustained
  client load and verify that foreground SLOs and convergence guarantees
  hold.

Property-based tests SHOULD assert: for any sequence of topology events
applied in any order, the final ownership map and per-key HLC histories
on every surviving replica are identical.

## Security Constraints

- Every `rebalance.plan`, `rebalance.transfer`, and `rebalance.commit`
  envelope MUST traverse a mutually authenticated TLS channel established
  on `servers.peer` (see `docs/architecture.md` — *Dual-endpoint model*).
- The peer certificate identity MUST be validated against the ring
  membership: a node not present in the current ring epoch MUST NOT be
  accepted as a transfer peer.
- Integrity digests are an end-to-end correctness check, not a security
  control; transport integrity is provided by mTLS.

## Open Questions

- Should plan folding across more than two adjacent epochs be enabled by
  default, or gated behind an operator flag for predictability?
- Should the dual-ownership window have a configurable maximum duration
  after which the source forcibly aborts the transfer to avoid indefinite
  fan-out?
- What is the right default rate-limit fraction (e.g. 25 % of outbound
  bandwidth) and how should it adapt to observed foreground latency?
- Should integrity digests be computed incrementally per batch or once at
  the end of the partition transfer? Incremental digests bound retry cost
  but increase steady-state CPU.
- How are virtual nodes (vnodes) accounted for in plan size and
  rate-limiting decisions?
- **Protocol kind for plan acceptance**: `docs/protocol.md` defines no
  `rebalance.plan.ack` kind; the target's acceptance path is unspecified
  — only rejection via the generic `error` kind is implied. Should a
  dedicated acknowledgement kind be added to the well-known kind registry,
  or is the absence of an `error` response sufficient to signal acceptance
  and allow the source to begin streaming?
- **Certificate revocation mid-rebalance**: `docs/security.md` requires a
  revocation strategy (CRL, OCSP, or short-lived certificates) but this
  document does not specify the expected behavior when a peer certificate
  is revoked while a transfer is in flight. Should the transfer abort
  immediately on revocation discovery, or is it permissible to complete a
  session that was fully authenticated at plan-negotiation time?
- **`LocalStoragePort` quarantine extension contract**: the *Network
  partition during rebalance* scenario requires a quarantined staging area
  — partial transfer batches MUST NOT be visible to reads until an atomic
  promote. This implies new methods on `LocalStoragePort`, but neither
  this document nor `docs/architecture.md` defines their interface,
  visibility semantics, or atomicity guarantees. The normative contract for
  quarantine staging and atomic promotion needs a formal home.
- **Ordering of concurrent writes at an incomplete target**: a write may
  arrive at the target for a key whose historical versions have not yet
  been transferred. The append-only apply path in `docs/convergence.md`
  requires prior HLC context to correctly sequence the incoming version
  relative to the in-progress transfer. This document does not state
  whether the target must buffer such writes, apply them speculatively, or
  reject them until the partition transfer completes.
- **Quorum arithmetic across heterogeneous replica sets**: the
  dual-ownership write path fans out to the union of old and new replica
  sets and counts acknowledgements from either side toward the configured
  consistency level. The spec does not define how quorum is computed when
  both replica sets are simultaneously degraded (e.g., a concurrent join
  and crash reduce each side to fewer than ⌊N/2⌋+1 reachable nodes). A
  formal quorum definition covering this cross-set degraded case is needed.

## Done Criteria

- The deterministic plan derivation is implemented behind a `RingPort`
  contract and produces identical output on every node for the same epoch
  pair.
- `rebalance.plan`, `rebalance.transfer`, and `rebalance.commit` are
  fully implemented with integrity validation and idempotent replay.
- The dual-ownership window preserves availability and quorum semantics
  across handover, validated by automated tests.
- Crash, partition, and storm scenarios listed under *Worst-Case
  Scenarios* are covered by automated fault-injection tests and pass
  reliably.
- Observability events and metrics listed under *Observability* are
  emitted and consumed by the `tourctl ring inspect` and structured-log
  paths.
- All rebalance traffic is verified to traverse mTLS exclusively, with
  certificate identity tied to ring membership.
