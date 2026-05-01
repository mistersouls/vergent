# Architecture — Milestone 2 Phase 2b: Replication and Hinted Handoff

**Status**: design (pre-implementation)
**Author**: Architect agent
**Normative inputs**: `docs/roadmap.md` §Phase 2b, `docs/ring.md`, `docs/membership.md`, `docs/architecture-ring-2a.md`

---

## Plan

Build the replication and hinted-handoff subsystem on top of the Phase 2a ring.
A node receiving a client write becomes the *coordinator* for that write,
computes a `preference_list` from its current ring snapshot, and fans the
write out to every replica in parallel via `asyncio.TaskGroup`. The coordinator
**may or may not be in the preference list**: when it is, it short-circuits a
local write that counts as one ack and then fans out to the remaining
replicas; when it is not (pure proxy), it stores nothing locally and acts as
a pure orchestrator over the replicas in the preference list. Quorum is
established when `W` acknowledgements have been collected. For replicas that
fail to acknowledge within their per-request budget (unreachable, slow, or in
`JOINING`/`IDLE`), the coordinator **delegates** a hint to one of the ackting
replicas (the *keeper*) via a `handoff.delegate` envelope; the keeper stores
the hint in **its own** local hinted handoff queue, ordered by the original
`HLCTimestamp`, and replays it deterministically once the target rejoins. The
coordinator itself stores no hints — this removes the durability hole where a
coordinator crash after quorum but before replay would lose hint state. The
whole path is async, leaderless, mTLS-only, and idempotent on replay.

---

## Scope Clarification

### What is in scope for Phase 2b

| Item | Location | Status |
|------|----------|--------|
| `ReplicationPort` Protocol | `core/ports/replication.py` | Create |
| `HandoffPort` Protocol | `core/ports/handoff.py` | Create |
| `ReplicaCoordinator` — leaderless fan-out, quorum collection, backpressure | `core/structure/replication.py` | Create |
| `HintedHandoffQueue` — HLC-ordered persistent deferral + idempotent replay | `core/structure/handoff.py` | Create |
| Replication kind handlers (`kv.replicate`, `kv.replicate.ok`) | `core/handlers/replication.py` | Create |
| Hinted-handoff kind handlers (`handoff.push`, `handoff.ack`) | `core/handlers/handoff.py` | Create |
| Hint-delegation kind handlers (`handoff.delegate`, `handoff.delegate.ok`) | `core/handlers/handoff.py` | Create |
| Package init for the new sub-package | `core/handlers/__init__.py` | Create |
| In-memory `HandoffPort` adapter for tests | `infra/memory/handoff.py` | Create |

### What is explicitly NOT in scope

- **Read-path quorum / read-repair**: Phase 2b focuses on the write path
  (`docs/roadmap.md` §Phase 2b deliverables). Read coordination and read-repair
  are deferred to a follow-up phase; the `ReplicaCoordinator` exposes only
  `replicate_write` / `replicate_delete` in this phase.
- **Anti-entropy / Merkle reconciliation**: hinted handoff covers *temporary*
  unavailability only. Long-term divergence repair (Merkle trees, full sync) is
  out of scope.
- **Partition rebalance plumbing**: Phase 2c / Milestone 3.
- **Proxy reads beyond simple forwarding**: a single-hop proxy for keys whose
  primary is not the receiving node is included via the existing `kv.put` /
  `kv.get` handlers chained into `ReplicationPort.replicate_write`. No
  multi-hop proxy chain.
- **Coordinator durability**: an in-flight write that has not reached `W` acks
  is *not* recovered if the coordinator crashes mid-fan-out. Clients retry.
  Hinted handoff persists *successful* but late-delivered writes only.
- **Gossip-driven hint flush triggers**: Phase 2b uses a periodic poll plus an
  explicit `replay(target_node_id)` entry point; gossip wiring is left to the
  bootstrap layer.
- **`TopologyAwarePlacementStrategy`**: still reserved.
- **New ring or membership types**: ring/membership remain frozen as shipped
  in Phase 2a.

---

## Target Architecture

```
tourillon/core/
├── structure/
│   ├── clock.py               (existing — HLCTimestamp, HLCClock)
│   ├── envelope.py            (existing — Envelope)
│   ├── version.py             (existing — StoreKey, Version, Tombstone)
│   ├── membership.py          (existing — MemberPhase, Member)
│   ├── replication.py         (NEW — ReplicaCoordinator, QuorumOutcome,
│   │                                   ReplicaAck, ReplicationRequest)   ← coordinator
│   └── handoff.py             (NEW — Hint, HintedHandoffQueue)            ← deferral
│
├── ring/                      (existing — Phase 2a, unchanged)
│
├── ports/
│   ├── storage.py             (existing)
│   ├── transport.py           (existing)
│   ├── serializer.py          (existing)
│   ├── replication.py         (NEW — ReplicationPort)                     ← driving port
│   └── handoff.py             (NEW — HandoffPort)                         ← driven port
│
├── handlers/                  (NEW sub-package — server-side kind routers)
│   ├── __init__.py            (re-exports kind constants and handler classes)
│   ├── replication.py         (ReplicationHandlers: kv.replicate)
│   └── handoff.py             (HandoffHandlers: handoff.push, handoff.ack)
│
├── dispatch.py                (existing — Dispatcher)
└── config.py                  (existing — TourillonConfig)

tourillon/infra/
└── memory/
    └── handoff.py             (NEW — InMemoryHandoffStore, test/dev adapter)
```

Hexagonal layer assignment:

```
Domain (pure async value/coordination objects, no TCP/TLS imports):
  core/structure/replication.py        (ReplicaCoordinator and friends)
  core/structure/handoff.py            (HintedHandoffQueue, Hint)

Application ports (Protocol contracts for adapters):
  core/ports/replication.py            (ReplicationPort — driving)
  core/ports/handoff.py                (HandoffPort — driven, persistence)

Application handlers (kind routers — pure async, depend only on ports):
  core/handlers/replication.py
  core/handlers/handoff.py

Infrastructure adapters (concrete I/O):
  infra/memory/handoff.py              (in-memory HandoffPort impl)
```

`ReplicaCoordinator` and `HintedHandoffQueue` live in `core/structure/`
because they are pure domain coordination objects: they are async (the domain
is intrinsically concurrent — quorum cannot be expressed synchronously) but
they carry **no TCP, no TLS, no socket, no msgpack** import. They consume
abstract `ReplicationPort` / `HandoffPort` instances, exactly as the ring
layer consumes `MemberPhase` without importing the gossip engine.

---

## Module Map and Responsibilities

### `tourillon/core/ports/replication.py`

**Responsibility**: define the `ReplicationPort` driving Protocol — the single
entry point through which a client-facing handler asks the coordinator to
replicate a write or a delete to the preference list, awaiting quorum.

**Placement justification**: every cross-process contract in Tourillon is
expressed as a `Protocol` in `core/ports/`. `ReplicationPort` is the natural
sibling of `LocalStoragePort`: where `LocalStoragePort` writes to *one*
local backing store, `ReplicationPort` orchestrates writes to *N replicas*.
Both are driving ports invoked from the same place (the kv handler chain) so
they belong in the same directory.

---

### `tourillon/core/ports/handoff.py`

**Responsibility**: define the `HandoffPort` driven Protocol — the
persistence-side contract that a `HintedHandoffQueue` uses to durably enqueue
and dequeue `Hint` records. The queue is the orchestrator; the port is the
storage adapter.

**Placement justification**: `HandoffPort` is to hints what
`LocalStoragePort` is to versions. It is a driven persistence port consumed
by the domain (`HintedHandoffQueue`) and implemented by infra adapters
(`infra/memory/handoff.py`, future on-disk variants).

---

### `tourillon/core/structure/replication.py`

**Responsibility**: own the `ReplicaCoordinator` async domain object plus the
small dataclasses it produces and consumes (`ReplicationRequest`,
`ReplicaAck`, `QuorumOutcome`). The coordinator:

1. Receives a `ReplicationRequest` carrying the original `WriteOp` /
   `DeleteOp`, the **frozen** `preference_list`, and the **frozen**
   `HLCTimestamp` already stamped by the local store.
2. Acquires a slot from a bounded `asyncio.Semaphore` (configured backpressure
   ceiling, see §Critical Invariants #11).
3. Branches on whether `local_node_id` is in `preference_list`:
   - **In the preference list**: short-circuit a local write (counts as 1
     ack), then fan out outbound calls to the remaining replicas via
     `asyncio.TaskGroup`.
   - **Not in the preference list (pure proxy)**: store nothing locally; fan
     out outbound calls to every replica in `preference_list`.
4. Collects replica acks until `W` are gathered or the request budget elapses.
5. For every replica that did not ack, **delegates** the hint to the first
   ackting replica in preference-list order by invoking the injected
   `delegate_hint(keeper_node_id, target_node_id, request)` callable, which
   sends a `handoff.delegate` envelope. The coordinator does NOT enqueue
   hints in any local queue — the keeper stores them in its own
   `HintedHandoffQueue`. Delegation is skipped entirely when quorum was not
   reached (no durable keeper exists).
6. Returns a `QuorumOutcome(success: bool, acked: list[ReplicaAck],
   hinted: list[str])`.

**Placement justification**: `core/structure/` is the canonical home for
domain types (clock, envelope, version, membership). `ReplicaCoordinator` is
async but pure-domain: it speaks only to `ReplicationPort` (for outbound
calls), an injected `delegate_hint` callable (for hint delegation), and
`LocalStoragePort` (for the self-replica path when the coordinator is in the
preference list). It does not import `HintedHandoffQueue` — hints are stored
on keeper replicas, not on the coordinator. It does not import asyncio I/O
primitives such as streams or sockets — only `asyncio.TaskGroup`,
`asyncio.Semaphore`, `asyncio.timeout`, and `asyncio.gather`.

---

### `tourillon/core/structure/handoff.py`

**Responsibility**: own the `Hint` immutable value object and the
`HintedHandoffQueue` async coordinator. The queue:

- Persists hints via `HandoffPort`, keyed by `(target_node_id, HLCTimestamp)`,
  so that natural `bisect` ordering on `HLCTimestamp` (which is `order=True`)
  yields deterministic replay order.
- Exposes `enqueue(hint)`, `pending_for(target_node_id) -> list[Hint]`, and
  `replay(target_node_id, send_one)` where `send_one` is an injected async
  callable that performs the actual `handoff.push` round-trip.
- Replays hints in strict ascending `HLCTimestamp` order. A hint is removed
  from the store **only after** the corresponding `handoff.ack` has been
  observed for its `correlation_id`.
- Idempotency is delegated to the receiving node: `handoff.push` re-applies
  the original `WriteOp` via `LocalStoragePort.put`, and the existing HLC
  precedence in the store ensures a re-applied hint is a no-op.

**Placement justification**: same as `replication.py`. Pure async coordinator,
no transport imports.

---

### `tourillon/core/handlers/replication.py`

**Responsibility**: server-side `KindHandler` implementations for the
`kv.replicate` envelope kind (and its `kv.replicate.ok` / `kv.err` responses).
Decodes the inbound envelope payload, applies the carried `WriteOp` /
`DeleteOp` to the *local* `LocalStoragePort` (the receiving node is one of
the replicas), and emits a `kv.replicate.ok` envelope carrying the local
acknowledgement metadata.

**Placement justification**: introduces a new `core/handlers/` sub-package as
the home for *core-level* kind handlers — handlers whose only dependencies
are core ports, not bootstrap wiring. The existing `tourillon/bootstrap/
handlers.py` (`KvHandlers`) remains the home for client-facing handlers and
keeps its bootstrap-layer responsibilities. Replication and handoff are
node-to-node protocol surfaces, not client surfaces, so they justify a
distinct namespace under `core/`.

---

### `tourillon/core/handlers/handoff.py`

**Responsibility**: server-side `KindHandler` implementations for
`handoff.push` (apply a deferred write to local storage; respond with
`handoff.ack`) and `handoff.ack` (callback path; logged for tracing only —
the receiving side does not act on it; the *sender* of `handoff.push`
correlates acks via the envelope's `correlation_id`).

**Placement justification**: same rationale as
`core/handlers/replication.py`.

---

### `tourillon/core/handlers/__init__.py`

**Responsibility**: re-export the kind constants and handler classes so that
callers import from `tourillon.core.handlers` without knowing the file
layout, mirroring the convention established by `tourillon.core.ring`.

---

### `tourillon/infra/memory/handoff.py`

**Responsibility**: in-memory `HandoffPort` implementation backed by a
per-target `list[Hint]` kept sorted by `HLCTimestamp` via `bisect.insort`.
Used by tests and single-node development. Production adapters (RocksDB,
SQLite WAL, etc.) are out of scope for this phase but will satisfy the same
`HandoffPort` Protocol unchanged.

**Placement justification**: mirrors `infra/memory/store.py` (the existing
in-memory `LocalStoragePort` adapter). All adapter code lives under
`tourillon/infra/`, never in `core/`.

---

## Interface / Protocol Contracts

### `core/ports/replication.py`

```python
from typing import Protocol

from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.version import Tombstone, Version


class QuorumNotReachedError(Exception):
    """Raised by ReplicationPort when fewer than W replicas acknowledged.

    Carries the partial ack count and the list of node_ids that did ack so
    that the caller can decide whether to surface a degraded-mode response or
    raise to the client. The hint queue has already been populated for every
    replica that did not ack by the time this error is raised.
    """

    def __init__(self, acked: int, required: int, ack_node_ids: list[str]) -> None:
        super().__init__(
            f"quorum not reached: {acked}/{required} acks "
            f"from {ack_node_ids!r}"
        )
        self.acked: int = acked
        self.required: int = required
        self.ack_node_ids: list[str] = ack_node_ids


class ReplicationPort(Protocol):
    """Driving port through which a coordinator-side handler replicates a write.

    The ReplicationPort hides the fan-out, quorum, timeout, and hint-enqueue
    bookkeeping behind two methods. Callers supply only the storage operation,
    the preference list (already computed from the ring snapshot the request
    arrived on), and the local HLC timestamp that was assigned by the local
    store. The port returns the local Version/Tombstone exactly once quorum
    is reached.

    All concrete implementations must be async, must not block the event loop,
    and must guarantee that on return the hint queue is consistent with the
    set of replicas that did and did not ack.
    """

    async def replicate_write(
        self,
        op: WriteOp,
        version: Version,
        preference_list: list[str],
    ) -> Version:
        """Fan the locally-stamped Version out to every replica in preference_list.

        version.metadata is the canonical HLCTimestamp for this write — the
        same timestamp is carried verbatim to every replica and to every hint
        produced by this call. Implementations must NOT re-stamp on the
        replica side.

        Returns the local Version once W acks (including the local replica)
        have been collected. Raises QuorumNotReachedError when the budget
        elapses before W acks are gathered. Replicas that failed to ack are
        already enqueued as hints by the time this method returns or raises.
        """
        ...

    async def replicate_delete(
        self,
        op: DeleteOp,
        tombstone: Tombstone,
        preference_list: list[str],
    ) -> Tombstone:
        """Same contract as replicate_write but for Tombstones.

        tombstone.metadata is the authoritative HLCTimestamp for the deletion.
        """
        ...
```

---

### `core/ports/handoff.py`

```python
from typing import Protocol

from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint


class HandoffPort(Protocol):
    """Driven port through which HintedHandoffQueue persists deferred writes.

    Implementations must:
    - Order hints per target_node_id by ascending HLCTimestamp.
    - Survive process restart (production impls); the in-memory adapter is
      restart-volatile and is for tests/dev only.
    - Never deduplicate by content. Idempotency on replay is the *receiver's*
      responsibility via HLC precedence in LocalStoragePort.
    - Be safe to call concurrently across coroutines for distinct targets.
    """

    async def append(self, hint: Hint) -> None:
        """Durably append hint, maintaining HLC order for hint.target_node_id."""
        ...

    async def pending(self, target_node_id: str) -> list[Hint]:
        """Return all hints for target_node_id sorted by ascending HLCTimestamp.

        The returned list is a snapshot; the underlying store may grow
        concurrently. Callers iterate the snapshot when replaying.
        """
        ...

    async def remove(self, target_node_id: str, ts: HLCTimestamp) -> None:
        """Remove the hint identified by (target_node_id, ts).

        No-op when the hint is absent (idempotent removal — supports re-acks
        from a replica that processed a push twice).
        """
        ...

    async def targets(self) -> frozenset[str]:
        """Return the set of node_ids that currently have at least one pending hint."""
        ...
```

---

### `core/structure/replication.py`

```python
import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from tourillon.core.ports.storage import DeleteOp, LocalStoragePort, WriteOp
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.version import Tombstone, Version


@dataclass(frozen=True)
class ReplicaAck:
    """Single replica's acknowledgement of a replicate request.

    node_id is the acknowledging replica. ts is the HLCTimestamp the replica
    recorded for the write — must equal the coordinator's ts (no re-stamping).
    """
    node_id: str
    ts: HLCTimestamp


@dataclass(frozen=True)
class QuorumOutcome:
    """Result of a single replicate_write / replicate_delete fan-out.

    success is True iff len(acked) >= W. hinted lists the node_ids that did
    not ack and whose Hint has been persisted by the time this object is
    constructed.
    """
    success: bool
    acked: tuple[ReplicaAck, ...]
    hinted: tuple[str, ...]


@dataclass(frozen=True)
class ReplicationRequest:
    """Frozen input snapshot for one fan-out.

    op is either a WriteOp or a DeleteOp. ts is the canonical HLCTimestamp
    already assigned by the local store; it is forwarded verbatim to all
    replicas and to all hints. preference_list is the snapshot computed from
    the ring view that was current when the client request arrived; it is
    NOT recomputed mid-replication (see Critical Invariant #6).
    """
    op: WriteOp | DeleteOp
    kind: Literal["put", "delete"]
    ts: HLCTimestamp
    preference_list: tuple[str, ...]


# Outbound replicate-call signature: takes a target node_id and a
# ReplicationRequest, returns a ReplicaAck on success or raises on failure
# (timeout, refusal, transport error).
ReplicateOne = Callable[[str, ReplicationRequest], "asyncio.Future[ReplicaAck]"]


# Hint delegation callable: instructs a keeper replica to durably store a
# hint for target_node_id by sending a handoff.delegate envelope. Awaits the
# keeper's handoff.delegate.ok response. Raises on transport/timeout failure.
DelegateHint = Callable[[str, str, ReplicationRequest], Awaitable[None]]


class ReplicaCoordinator:
    """Async leaderless write coordinator.

    The node receiving the client request constructs one ReplicaCoordinator
    per process (shared across requests) and invokes replicate_write /
    replicate_delete for each client write. There is no global coordinator.
    The coordinator may or may not itself be in the preference list for a
    given key — both paths are supported and the coordinator role is
    ephemeral per-request.

    Concurrency model:
    - asyncio.Semaphore caps the number of in-flight fan-outs (backpressure).
    - asyncio.TaskGroup is used inside each fan-out to launch one outbound
      task per replica; the group is cancelled cleanly on quorum, on budget
      exhaustion, or on an unrecoverable exception.
    - asyncio.timeout enforces the per-request budget.
    """

    def __init__(
        self,
        *,
        local_node_id: str,
        replicate_one: ReplicateOne,
        delegate_hint: DelegateHint,
        write_quorum: int,
        max_in_flight: int,
        request_budget_ms: int,
    ) -> None:
        """
        local_node_id  — this node's identifier; used to detect whether the
                         coordinator is in the preference list and to short-
                         circuit local replica writes.
        replicate_one  — outbound replicate callable (unchanged).
        delegate_hint  — callable that sends handoff.delegate to a keeper:
                         delegate_hint(keeper_node_id, target_node_id, request).
                         The coordinator calls this for each non-acking replica,
                         choosing the first ackting replica as keeper.
                         Must not be called when quorum was not reached (no
                         ackting replica is available to keep the hint durably).
        write_quorum   — W (unchanged).
        max_in_flight  — Semaphore bound (unchanged).
        request_budget_ms — per-request deadline (unchanged).
        """

    async def replicate_write(
        self,
        op: WriteOp,
        version: Version,
        preference_list: list[str],
    ) -> Version:
        """Implements ReplicationPort.replicate_write.

        Procedure:
          1. Acquire the in-flight semaphore (backpressure).
          2. Build a ReplicationRequest carrying op, version.metadata, and
             tuple(preference_list).
          2a. If local_node_id is in preference_list, perform the local
             replica write via the upstream LocalStoragePort path (already
             completed by the caller — version is the result) and count it
             as one ack immediately. If local_node_id is NOT in
             preference_list (pure proxy), no local write is performed and
             every ack must come from an outbound call.
          3. Inside `async with asyncio.timeout(budget)` and
             `async with asyncio.TaskGroup() as tg`, launch one task per
             remaining replica via tg.create_task(self._call_one(...)).
          4. Collect ReplicaAcks via an internal asyncio.Queue. As soon as W
             acks are gathered, cancel the remaining tasks (TaskGroup.cancel
             via raising a sentinel) but DO NOT cancel the delegation path —
             outstanding replicas still get a hint delegated.
          5. For each non-acking replica r, select keeper = first ackting
             replica in preference_list order, then await
             delegate_hint(keeper, r, request). The coordinator stores
             nothing locally; the keeper is responsible for durability.
          6. Return version on success; raise QuorumNotReachedError when
             len(acked) < W. In that case do NOT call delegate_hint — there
             is no durable keeper available, so no hint is delegated.
        """

    async def replicate_delete(
        self,
        op: DeleteOp,
        tombstone: Tombstone,
        preference_list: list[str],
    ) -> Tombstone:
        """Same procedure as replicate_write, with Tombstone in place of Version.

        Step 2a, step 5 (keeper-delegated hints), and step 6 (no delegation
        on QuorumNotReachedError) apply identically.
        """

    async def _call_one(
        self,
        target_node_id: str,
        request: ReplicationRequest,
        ack_queue: asyncio.Queue[ReplicaAck | BaseException],
    ) -> None:
        """Per-replica task body. Runs replicate_one or local short-circuit;
        on success enqueues the ReplicaAck on ack_queue; on failure enqueues
        the exception so the gather loop can record the target as hint-eligible.
        Never re-raises; failure is a normal data-flow event."""
```

---

### `core/structure/handoff.py`

```python
import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from tourillon.core.ports.handoff import HandoffPort
from tourillon.core.ports.storage import DeleteOp, WriteOp
from tourillon.core.structure.clock import HLCTimestamp


@dataclass(frozen=True)
class Hint:
    """Persistent record of a write that could not be replicated synchronously.

    target_node_id — the replica that owes us this write.
    ts             — the ORIGINAL HLCTimestamp of the deferred write. Carried
                     verbatim into the handoff.push payload so that the
                     receiver applies the same causal timestamp it would have
                     received during the live fan-out (Critical Invariant #4).
    op             — WriteOp xor DeleteOp; replayed via LocalStoragePort.put
                     or .delete on the receiver.
    kind           — discriminator: "put" or "delete".
    """
    target_node_id: str
    ts: HLCTimestamp
    op: WriteOp | DeleteOp
    kind: str  # Literal["put", "delete"]


# Adapter callable: send a single handoff.push for hint, return True on ack.
PushOne = Callable[[Hint], Awaitable[bool]]


class HintedHandoffQueue:
    """Async coordinator over a HandoffPort.

    Responsibilities:
    - Provide the only API the ReplicaCoordinator uses to defer a write.
    - Maintain replay determinism: hints for a given target_node_id are
      replayed in strict ascending HLCTimestamp order.
    - Remove a hint from the underlying store ONLY after the corresponding
      handoff.ack is observed; a failed replay leaves the hint in place for
      the next attempt.
    - Be safe to call concurrently for distinct targets. Per-target replay
      is serialised via an asyncio.Lock so that two replays for the same
      target cannot interleave and break ordering.
    """

    def __init__(self, store: HandoffPort) -> None: ...

    async def enqueue(
        self,
        target_node_id: str,
        ts: HLCTimestamp,
        op: WriteOp | DeleteOp,
        kind: str,
    ) -> None:
        """Build a Hint and persist it via HandoffPort.append.

        Precondition: ts is the original HLCTimestamp, never a re-stamped one.
        """

    async def pending_for(self, target_node_id: str) -> list[Hint]:
        """Snapshot of hints for target, sorted ascending by HLCTimestamp."""

    async def replay(self, target_node_id: str, push_one: PushOne) -> int:
        """Replay every pending hint for target_node_id in HLC order.

        For each hint h (ascending ts):
            ok = await push_one(h)
            if ok: await self._store.remove(target_node_id, h.ts)
            else:  break  # leave remaining hints in place for next replay

        Returns the number of hints successfully delivered. Serialised
        per-target via an asyncio.Lock so concurrent replays for the same
        target are safe.
        """

    async def known_targets(self) -> frozenset[str]:
        """Pass-through to HandoffPort.targets()."""
```

---

### Envelope kinds

All four kinds are ≤ 64 bytes UTF-8 (the `Envelope.KIND_MAX_LEN`
constraint) and follow the existing `kv.*` / `handoff.*` namespacing.

| kind | Direction | Payload (msgpack) | Response kind |
|------|-----------|-------------------|---------------|
| `kv.replicate` | coordinator → replica | `{op_kind: "put"\|"delete", keyspace: bytes, key: bytes, value?: bytes, wall: int, counter: int, node_id: str}` | `kv.replicate.ok` or `kv.err` |
| `kv.replicate.ok` | replica → coordinator | `{wall: int, counter: int, node_id: str}` (mirrors the applied HLCTimestamp; MUST equal the coordinator's ts) | — |
| `handoff.push` | sender → target | identical schema to `kv.replicate` (the hint is just a deferred replicate) | `handoff.ack` or `kv.err` |
| `handoff.ack` | target → sender | `{wall: int, counter: int, node_id: str}` | — |
| `handoff.delegate` | coordinator → keeper | same fields as `kv.replicate` plus `target_node_id: str` (the replica that ultimately owes the write) | `handoff.delegate.ok` or `kv.err` |
| `handoff.delegate.ok` | keeper → coordinator | `{wall: int, counter: int, node_id: str}` (keeper's HLC confirmation that the hint is durably stored) | — |

`correlation_id` on the response always equals the request's
`correlation_id`, matching the existing `kv.put` / `kv.put.ok` convention in
`tourillon/bootstrap/handlers.py`.

---

### `core/handlers/replication.py`

```python
import logging
from collections.abc import Callable

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.ports.storage import DeleteOp, LocalStoragePort, WriteOp
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.version import StoreKey

KIND_KV_REPLICATE: str = "kv.replicate"
KIND_KV_REPLICATE_OK: str = "kv.replicate.ok"

_logger = logging.getLogger(__name__)


class ReplicationHandlers:
    """Server-side handlers for the kv.replicate protocol kind.

    A node registers ReplicationHandlers with its Dispatcher to act as a
    *replica* in another node's fan-out. The handler decodes the inbound
    envelope, applies the carried op to the local LocalStoragePort using the
    SAME HLCTimestamp the coordinator stamped (no re-stamping — Invariant #4),
    and emits a kv.replicate.ok envelope echoing that timestamp.

    Idempotency: the local store's HLC precedence (see Version.metadata
    comparison in core/structure/version.py) ensures that re-applying the
    same (address, ts) pair is a no-op. ReplicationHandlers therefore does
    not need to track seen correlation_ids.
    """

    def __init__(
        self,
        store: LocalStoragePort,
        serializer: SerializerPort,
    ) -> None: ...

    def register(self, dispatcher: Dispatcher) -> None:
        """Register handle_replicate on dispatcher under KIND_KV_REPLICATE."""

    async def handle_replicate(
        self, envelope: Envelope, send: SendCallback
    ) -> None:
        """Decode, apply (put or delete) at the carried ts, respond ok or err.

        On success the response payload is {wall, counter, node_id} mirroring
        the request's HLCTimestamp. On failure (decode error, storage error)
        the response is a kv.err envelope.
        """
```

---

### `core/handlers/handoff.py`

```python
import logging

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.ports.storage import LocalStoragePort
from tourillon.core.ports.transport import SendCallback
from tourillon.core.structure.envelope import Envelope
from tourillon.core.structure.handoff import HintedHandoffQueue

KIND_HANDOFF_PUSH: str = "handoff.push"
KIND_HANDOFF_ACK: str = "handoff.ack"
KIND_HANDOFF_DELEGATE: str = "handoff.delegate"
KIND_HANDOFF_DELEGATE_OK: str = "handoff.delegate.ok"

_logger = logging.getLogger(__name__)


class HandoffHandlers:
    """Server-side handlers for handoff.push, handoff.ack, and handoff.delegate.

    handoff.push is functionally identical to kv.replicate: apply the carried
    op at the carried HLCTimestamp via LocalStoragePort. The handler MUST
    accept hints in any order (they may arrive out of HLC order if a sender
    parallelises across targets) and rely on store HLC precedence for
    idempotency.

    handoff.ack arriving inbound on a replica is unexpected (acks travel
    sender ← target); the handler logs at WARNING and discards.

    handoff.delegate makes this node a *keeper*: it persists a hint for some
    other replica (target_node_id, named in the payload) into its local
    HintedHandoffQueue and acks via handoff.delegate.ok. The keeper later
    replays the hint via handoff.push when target_node_id becomes reachable.
    The keeper requires its own HintedHandoffQueue, hence the queue is
    injected here (it is NOT injected into ReplicaCoordinator).
    """

    def __init__(
        self,
        store: LocalStoragePort,
        serializer: SerializerPort,
        handoff_queue: HintedHandoffQueue,
    ) -> None: ...

    def register(self, dispatcher: Dispatcher) -> None:
        """Register handle_push under KIND_HANDOFF_PUSH, handle_ack under
        KIND_HANDOFF_ACK, and handle_delegate under KIND_HANDOFF_DELEGATE."""

    async def handle_push(
        self, envelope: Envelope, send: SendCallback
    ) -> None:
        """Apply the carried op at the carried HLCTimestamp, respond handoff.ack."""

    async def handle_ack(
        self, envelope: Envelope, send: SendCallback
    ) -> None:
        """Log and discard. Acks are correlated by the sender, not by us."""

    async def handle_delegate(
        self, envelope: Envelope, send: SendCallback
    ) -> None:
        """Receive a handoff.delegate: store hint in local HintedHandoffQueue, respond handoff.delegate.ok.

        The envelope payload contains the same fields as kv.replicate plus
        target_node_id. This node becomes the keeper: it stores the hint for
        target_node_id and is responsible for replaying it via handoff.push
        when target_node_id becomes reachable again.
        """
```

---

### `core/handlers/__init__.py`

```python
"""Core handlers public surface for Milestone 2 Phase 2b."""

from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_ACK as KIND_HANDOFF_ACK,
)
from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_DELEGATE as KIND_HANDOFF_DELEGATE,
)
from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_DELEGATE_OK as KIND_HANDOFF_DELEGATE_OK,
)
from tourillon.core.handlers.handoff import (
    KIND_HANDOFF_PUSH as KIND_HANDOFF_PUSH,
)
from tourillon.core.handlers.handoff import HandoffHandlers as HandoffHandlers
from tourillon.core.handlers.replication import (
    KIND_KV_REPLICATE as KIND_KV_REPLICATE,
)
from tourillon.core.handlers.replication import (
    KIND_KV_REPLICATE_OK as KIND_KV_REPLICATE_OK,
)
from tourillon.core.handlers.replication import (
    ReplicationHandlers as ReplicationHandlers,
)

__all__ = [
    "KIND_KV_REPLICATE",
    "KIND_KV_REPLICATE_OK",
    "KIND_HANDOFF_PUSH",
    "KIND_HANDOFF_ACK",
    "KIND_HANDOFF_DELEGATE",
    "KIND_HANDOFF_DELEGATE_OK",
    "ReplicationHandlers",
    "HandoffHandlers",
]
```

---

### `infra/memory/handoff.py`

```python
import asyncio
import bisect
from collections import defaultdict

from tourillon.core.ports.handoff import HandoffPort
from tourillon.core.structure.clock import HLCTimestamp
from tourillon.core.structure.handoff import Hint


class InMemoryHandoffStore:
    """Process-local HandoffPort implementation for tests and dev.

    Holds a dict[node_id, list[Hint]] where each list is kept sorted by
    Hint.ts (HLCTimestamp.__lt__). One asyncio.Lock per target serialises
    mutations so that append/remove/pending observe consistent state.

    Restart-volatile by design. Production deployments must substitute a
    durable adapter that preserves HLC ordering across restarts.
    """

    def __init__(self) -> None: ...
    async def append(self, hint: Hint) -> None: ...
    async def pending(self, target_node_id: str) -> list[Hint]: ...
    async def remove(self, target_node_id: str, ts: HLCTimestamp) -> None: ...
    async def targets(self) -> frozenset[str]: ...
```

---

## Import DAG (no cycles, asyncio confined to coordinator/handlers/adapters)

```
core/structure/clock.py        (no new deps; existing)
core/structure/version.py      (no new deps; existing)
core/structure/membership.py   (no new deps; existing)

core/ports/storage.py          (existing)
core/ports/handoff.py
        ├── core/structure/clock.py
        └── core/structure/handoff.py     (forward ref for Hint type)

core/structure/handoff.py
        ├── core/ports/handoff.py         (HandoffPort consumed)
        ├── core/ports/storage.py         (WriteOp, DeleteOp)
        └── core/structure/clock.py
        — uses asyncio (Lock); no transport imports

core/ports/replication.py
        ├── core/ports/storage.py
        ├── core/structure/clock.py
        └── core/structure/version.py

core/structure/replication.py
        ├── core/ports/storage.py
        ├── core/ports/transport.py       (SendCallback type only)
        ├── core/structure/clock.py
        ├── core/structure/envelope.py
        └── core/structure/version.py
        — uses asyncio.TaskGroup, Semaphore, timeout, Queue; no socket/TLS
        — declares DelegateHint = Callable[[str, str, ReplicationRequest],
          Awaitable[None]] via collections.abc; does NOT import
          core/structure/handoff.py (no HintedHandoffQueue dependency)

core/handlers/replication.py
        ├── core/dispatch.py
        ├── core/ports/serializer.py
        ├── core/ports/storage.py
        ├── core/ports/transport.py
        ├── core/structure/clock.py
        ├── core/structure/envelope.py
        └── core/structure/version.py

core/handlers/handoff.py
        ├── core/dispatch.py
        ├── core/ports/serializer.py
        ├── core/ports/storage.py
        ├── core/ports/transport.py
        ├── core/structure/clock.py
        ├── core/structure/envelope.py
        ├── core/structure/handoff.py     (HintedHandoffQueue — keeper queue)
        └── core/structure/version.py

core/handlers/__init__.py
        └── all core/handlers/*

infra/memory/handoff.py
        ├── core/ports/handoff.py
        ├── core/structure/clock.py
        └── core/structure/handoff.py
```

`core/ports/handoff.py` references `Hint` from `core/structure/handoff.py`.
Resolve via `from __future__ import annotations` plus `if TYPE_CHECKING`
import (or inline the small `Hint` dataclass into the port module — chosen
during implementation; default is `TYPE_CHECKING` to keep value objects in
`structure/`).

---

## Critical Invariants

| # | Invariant | Enforcement |
|---|-----------|-------------|
| 1 | **No leader**: any node receiving a client request becomes the coordinator for that request. The coordinator may or may not itself be in the preference list — both modes are first-class, and the coordinator role is ephemeral per-request | `ReplicaCoordinator` is a per-node singleton injected at bootstrap; no global coordinator role; no leader-election code path anywhere; `replicate_write` branches on `local_node_id in preference_list` and either short-circuits a local replica write or acts as a pure proxy |
| 2 | **Quorum semantics: W + R > N** | `ReplicaCoordinator.__init__` validates `1 <= W <= max_replicas`; configuration layer validates `W + R > N` at startup; default deployment ships W=2, R=2, N=3 |
| 3 | **HLC ordering mandatory** | `ReplicationRequest.ts` is required and frozen; `Hint.ts` is required; replay walks `pending(target)` in ascending `HLCTimestamp` order; the receiving handler applies the carried ts verbatim |
| 4 | **Hint carries original HLC** | `HintedHandoffQueue.enqueue` accepts `ts` parameter and refuses None; the keeper (not the coordinator) enqueues hints with `request.ts`, never with a fresh `HLCClock.tick()` |
| 5 | **Idempotent replay** | Receiver applies `WriteOp` / `DeleteOp` via `LocalStoragePort` whose existing HLC precedence (cf. `core/structure/version.py`) makes re-application a no-op; `HandoffPort.remove` is also idempotent for repeated acks |
| 6 | **Ring-epoch coupling: preference_list frozen at request entry** | `ReplicationRequest.preference_list: tuple[str, ...]` is constructed once by the upstream handler from the ring snapshot in scope at that moment; `ReplicaCoordinator` accepts only this frozen tuple, never a `Ring` reference, so it cannot recompute mid-replication |
| 7 | **mTLS enforced — no plaintext fallback** | All inter-node envelopes (`kv.replicate`, `handoff.push`, `handoff.delegate`) flow through the existing `TcpServer` / `Connection` adapters whose `TlsHandshakeError` rejects unauthenticated peers; the new code introduces zero new transport listeners |
| 8 | **Backpressure: bounded in-flight fan-outs** | `ReplicaCoordinator` holds a single `asyncio.Semaphore(max_in_flight)`; every `replicate_*` call acquires before fan-out and releases in a `finally`; saturation manifests as awaiting acquire, never as unbounded task creation |
| 9 | **Asyncio fan-out via TaskGroup, not gather** | `ReplicaCoordinator._fan_out` uses `async with asyncio.TaskGroup()`; cancellation on quorum is structured; no orphaned tasks survive the call's exit |
| 10 | **Pure domain layer — no TCP/TLS imports in `structure/`** | `core/structure/replication.py` and `core/structure/handoff.py` import only `asyncio`, `dataclasses`, `tourillon.core.ports.*`, and `tourillon.core.structure.*`; verified by an import-linter rule (or, until then, by the per-batch grep check in §Validation) |
| 11 | **Envelope kind ≤ 64 bytes** | All six new kinds (`kv.replicate`, `kv.replicate.ok`, `handoff.push`, `handoff.ack`, `handoff.delegate`, `handoff.delegate.ok`) are short ASCII strings; the existing `Envelope.__post_init__` already enforces `KIND_MAX_LEN = 64` |
| 12 | **Hints removed only on ack (keeper-side)** | The keeper's `HintedHandoffQueue.replay` calls `HandoffPort.remove` only after `push_one(h)` returns `True`; on `False` the loop breaks, preserving order on the next attempt. This invariant applies to the keeper, since the coordinator no longer stores hints |
| 13 | **Per-target replay serialisation** | `HintedHandoffQueue` holds `dict[str, asyncio.Lock]` keyed by target_node_id; concurrent replays for the same target wait on the lock; concurrent replays for distinct targets proceed in parallel |
| 14 | **Hint delegated to ackting replica — never stored on coordinator** | `ReplicaCoordinator` does not import `HintedHandoffQueue` and holds no hint state; for each non-acking replica it invokes `delegate_hint(keeper, target, request)` where `keeper` is an ackting replica. A coordinator crash after quorum does not lose hint durability — the hint already lives on the keeper by the time `replicate_write` returns |
| 15 | **Max 1 hint copy per non-acking replica — deterministic keeper selection** | The coordinator selects `keeper = first ackting replica in preference_list order`. Selection is deterministic (no randomness) and produces exactly one keeper per non-acking target, so each hint exists in exactly one place across the cluster |
| 16 | **No hint delegation when quorum not reached** | When `len(acked) < W`, `ReplicaCoordinator` raises `QuorumNotReachedError` BEFORE invoking `delegate_hint` for any non-acking replica. Rationale: with no durable keeper, delegation would either fail or store hints on a node that itself failed to ack — providing no durability guarantee. The client must retry; HLC precedence makes the retry idempotent |

---

## Execution Order for Worker Agents

Implementation proceeds in five batches, each depending on all prior batches.
Maximum 3 files per batch.

### Batch 1 — Driven port + Hint value object (no deps on other new files)

| File | Key deliverable |
|------|----------------|
| `core/structure/handoff.py` (Hint dataclass only — class skeleton for `HintedHandoffQueue` deferred to Batch 3) | `Hint` frozen dataclass + field validation |
| `core/ports/handoff.py` | `HandoffPort` Protocol with all four async methods |
| `infra/memory/handoff.py` | `InMemoryHandoffStore` satisfying `HandoffPort` |

Tests:
- `Hint` validation: empty `target_node_id` → `ValueError`; non-`HLCTimestamp` `ts` → `TypeError`.
- `InMemoryHandoffStore.append` then `pending` returns hints sorted ascending by `ts`.
- `remove` of an absent `(target, ts)` is a no-op.
- `targets()` updates as hints are appended/removed.
- `isinstance(InMemoryHandoffStore(), HandoffPort)` (runtime-checkable).

---

### Batch 2 — Driving port + Replication value objects

| File | Key deliverable |
|------|----------------|
| `core/ports/replication.py` | `ReplicationPort` Protocol, `QuorumNotReachedError` |
| `core/structure/replication.py` (value objects only — `ReplicaCoordinator` body deferred to Batch 4) | `ReplicaAck`, `QuorumOutcome`, `ReplicationRequest` frozen dataclasses |

Tests:
- Frozen dataclass equality and hash for `ReplicaAck` / `ReplicationRequest`.
- `ReplicationRequest.preference_list` is a `tuple` (immutable) — passing a `list` is rejected at construction or coerced and verified.
- `QuorumNotReachedError(acked, required, node_ids)` carries fields.

---

### Batch 3 — `HintedHandoffQueue` (depends on Batches 1–2)

| File | Key deliverable |
|------|----------------|
| `core/structure/handoff.py` (queue class) | `HintedHandoffQueue.enqueue`, `.pending_for`, `.replay`, `.known_targets`, per-target `asyncio.Lock` |

Tests:
- `enqueue` with original HLC; `pending_for` returns hints in HLC order regardless of insertion order.
- `replay` with a `push_one` that always returns `True` removes all hints; with one that returns `False` mid-stream stops and leaves remaining hints in place.
- Concurrent `replay` calls for the same target serialise (one waits on the lock); for distinct targets they proceed in parallel (`asyncio.gather`).
- Replaying twice after the first replay drained everything is a no-op.

---

### Batch 4 — `ReplicaCoordinator` (depends on Batches 1–3)

| File | Key deliverable |
|------|----------------|
| `core/structure/replication.py` (coordinator class) | `ReplicaCoordinator.__init__(local_node_id, replicate_one, delegate_hint, write_quorum, max_in_flight, request_budget_ms)` — note: NO `local_store` and NO `handoff_queue` injected; `delegate_hint` replaces both. `replicate_write`, `replicate_delete`, `_call_one`, `asyncio.TaskGroup` fan-out, `asyncio.Semaphore` backpressure, hint delegation on miss |

Tests (each test injects a fake `replicate_one` and a fake `delegate_hint`
recorder; the coordinator no longer needs an `InMemoryHandoffStore`):
- All replicas ack within budget → `Version` returned; `delegate_hint` is never called.
- `W` of `N` replicas ack; remaining replicas time out → `Version` returned; `delegate_hint` is called once per non-acker, with the **first ackting replica in preference_list order** as keeper and the original `ts` carried in the request.
- Coordinator is in `preference_list` → local replica write short-circuits and counts as one ack; no outbound call is issued for the local node.
- Coordinator is NOT in `preference_list` (pure proxy) → no local write occurs; every ack must come from an outbound call; the coordinator stores nothing locally.
- Fewer than `W` replicas ack → `QuorumNotReachedError` is raised; `delegate_hint` is **NOT** called for any non-acking replica (no durable keeper exists).
- Semaphore enforcement: `max_in_flight=1`; two concurrent `replicate_write` calls observe strict serialisation.
- Coordinator never recomputes `preference_list`: pass a frozen tuple, mutate the source list, verify the in-flight call uses the original tuple (identity preserved).
- Cancellation: simulating an exception in one outbound task does not leak the other tasks (`TaskGroup` reports both).

---

### Batch 5 — Handlers + package surface (depends on Batches 1–4)

| File | Key deliverable |
|------|----------------|
| `core/handlers/replication.py` | `ReplicationHandlers.handle_replicate`, kind constants |
| `core/handlers/handoff.py` | `HandoffHandlers.handle_push`, `.handle_ack`, `.handle_delegate`, kind constants `KIND_HANDOFF_PUSH`, `KIND_HANDOFF_ACK`, `KIND_HANDOFF_DELEGATE`, `KIND_HANDOFF_DELEGATE_OK`; `__init__` accepts an injected `HintedHandoffQueue` (the keeper queue); `register` wires `handle_delegate` under `KIND_HANDOFF_DELEGATE` in addition to push/ack |
| `core/handlers/__init__.py` | Re-exports + `__all__` (including the two new delegate kind constants) |

Tests:
- `handle_replicate` decodes a payload with `(wall, counter, node_id)`, applies `WriteOp` to a `MemoryStore`, emits `kv.replicate.ok` whose payload mirrors the carried HLC.
- Re-applying the same payload (same ts, same value) is a no-op at the store; the handler still responds `kv.replicate.ok` (idempotent path).
- `handle_replicate` with a malformed payload responds `kv.err`, never raises out of the dispatch loop.
- `handle_push` is functionally equivalent to `handle_replicate` but emits `handoff.ack`.
- `handle_ack` logs at WARNING when received inbound and does not call `send`.
- `handle_delegate` decodes a payload carrying `target_node_id`, enqueues a `Hint` into the injected `HintedHandoffQueue` for that target with the carried HLC, and emits `handoff.delegate.ok`.
- `handle_delegate` with a malformed payload (missing `target_node_id`, etc.) responds `kv.err`.
- `correlation_id` is preserved verbatim from request to response across all three handlers.
- Round-trip integration: real `Dispatcher`, `ReplicationHandlers` registered, an inbound `kv.replicate` envelope produces a `kv.replicate.ok` envelope on the test send mock.

---

## Validation

### Per-file checklist (to run after each batch)

- `uv run black --check tourillon/` — zero formatting diff
- `uv run ruff check tourillon/` — zero violations (E W F I N UP B SIM C90)
- `uv run pytest tests/ --cov-fail-under=90` — all pass
- `get_errors` on each new file — no type errors
- `grep -RE "import (socket|ssl)|asyncio\.(open_connection|start_server)" tourillon/core/structure/` — empty output (Invariant #10)
- `grep -RE "from tourillon\.infra" tourillon/core/` — empty output (hexagonal boundary)
- `grep -RE "HintedHandoffQueue" tourillon/core/structure/replication.py` — empty output (Invariant #14: coordinator does not import the keeper queue)

### Quorum acceptance test (Phase 2b exit criterion)

With `N=3` replicas (`n1`, `n2`, `n3`), `W=2`, `R=2`, in-memory store and
in-memory handoff store on each:

```python
preference_list = ("n1", "n2", "n3")  # frozen snapshot from ring
op = WriteOp(address=StoreKey(b"ks", b"k"), value=b"v", now_ms=1000)
version = await n1_store.put(op)             # local stamping
result = await n1_coordinator.replicate_write(op, version, list(preference_list))
# All three reachable: result == version; len(handoff_store.targets()) == 0
```

Then simulate `n3` unavailable mid-fan-out (the injected `replicate_one`
raises `TimeoutError` for `n3`):

```python
result = await n1_coordinator.replicate_write(op2, version2, list(preference_list))
# W=2 acks from n1 (local) + n2: result == version2
# Hint persisted: (await handoff_store.pending("n3")) == [Hint(target="n3", ts=version2.metadata, ...)]
```

Bring `n3` back and replay:

```python
delivered = await n1_handoff_queue.replay("n3", push_one_real)
# delivered == 1
# (await handoff_store.pending("n3")) == []
# n3_store contains version2 with the SAME HLCTimestamp as n1 and n2
```

### Determinism acceptance test (replay order)

Enqueue three hints in reverse HLC order; replay must deliver them in
ascending HLC order:

```python
ts1 = HLCTimestamp(wall=100, counter=0, node_id="n1")
ts2 = HLCTimestamp(wall=200, counter=0, node_id="n1")
ts3 = HLCTimestamp(wall=300, counter=0, node_id="n1")
await q.enqueue("n3", ts3, op3, "put")
await q.enqueue("n3", ts1, op1, "put")
await q.enqueue("n3", ts2, op2, "put")
delivered_order: list[HLCTimestamp] = []
async def push_one(h: Hint) -> bool:
    delivered_order.append(h.ts); return True
await q.replay("n3", push_one)
assert delivered_order == [ts1, ts2, ts3]
```

---

## Risks and Trade-offs

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Coordinator crash mid-fan-out loses the in-flight write (no durable coordinator log) | Medium | Client must retry; partial replica state may exist | Documented in §Scope (NOT in scope for 2b); client retry is idempotent because `LocalStoragePort` is HLC-precedence safe. After quorum is reached AND `delegate_hint` has completed for every non-acker, hints survive on keeper replicas — coordinator crash no longer endangers hint durability. The remaining risk window is narrow: between quorum being established and `delegate_hint` completing |
| Keeper crashes between receiving `handoff.delegate` and durably storing the hint | Low | Hint lost for one non-acking replica; target eventually catches up via anti-entropy (out of scope for 2b) or via a retry of the original write | Same window as the prior coordinator-side risk, now scoped to the keeper rather than the coordinator. Mitigated by durable `HandoffPort` implementations (production deployments must wire a fsync-backed adapter); with the in-memory adapter the window is process-wide, as documented |
| `preference_list` frozen at request entry becomes stale during fan-out (node leaves between snapshot and fan-out) | Medium | Hint enqueued for a node that has fully left; replay never succeeds | Hints accumulate per target and a cleanup hook (out of scope for 2b) prunes hints whose target is `IDLE` for longer than a threshold; for now, hints remain pending — no data loss, only growing queue |
| `asyncio.Semaphore` saturation under load stalls all writes uniformly | Medium | Coordinated tail-latency spike across all clients | `max_in_flight` is configurable; operators size it from observed N × replica RTT × target TPS; degraded-mode is preferable to OOM |
| Hint replay storm when a long-absent node rejoins (thousands of pending hints) | Medium | Spike in handoff traffic, replica catch-up latency | Per-target `replay` is single-threaded by the asyncio.Lock and naturally rate-limited; future enhancement: chunked replay with explicit `batch_size` (deferred) |
| HandoffPort in-memory adapter loses hints on restart | High (in dev) | Data loss for *unacknowledged* writes only | Documented as test/dev only; production deployment must wire a durable adapter; the Protocol is unchanged |
| Two coordinators both replicate the same client write (client retry without idempotency token) | Medium | Same write applied twice on each replica | HLC precedence in `LocalStoragePort.put` makes the second application a no-op when the timestamp matches; if the client retried with a new `now_ms`, the second write supersedes the first — same as a normal subsequent write |
| `kv.replicate` and `kv.put` share the storage path — accidental client misuse of `kv.replicate` | Low | A client could bypass coordination by sending `kv.replicate` directly | mTLS layer enforces peer authentication; client certificates are distinct from node certificates in the bootstrap PKI; `ReplicationHandlers` may additionally check the peer identity (deferred — track in follow-up) |
| `TaskGroup` cancellation semantics on early-quorum exit may surface `CancelledError` traces | Low | Noisy logs | Use a sentinel exception caught at the `TaskGroup` boundary; do not log `CancelledError` propagated from the group |
| Replay for a target that briefly transitions `JOINING → READY` mid-replay races with normal writes | Low | Out-of-HLC-order apply on the replica | Replica HLC precedence on `LocalStoragePort.put` is the authoritative ordering — physical arrival order does not matter |
