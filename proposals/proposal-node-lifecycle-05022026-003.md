# Proposal: Node Lifecycle — Phase Model, State File, and Startup

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Draft
**Date:** 2026-05-02
**Sequence:** 003
**Schema version:** 1

---

## Summary

This proposal is the normative reference for a Tourillon node's operational
identity. It defines the complete `MemberPhase` finite-state machine (including
`PAUSED` as an interruptible maintenance state), the `Member` gossip value
object, the durable `state.toml` file that survives restarts, the startup
sequence, local failure detection, and `tourctl node inspect`. The join, leave,
and pause protocols that drive the transitions are specified in proposals 002,
004, and 005 respectively.

---

## Motivation

Without a well-defined, durable phase model the following problems are
unsolvable:

- **Crash recovery**: a restarted node cannot know whether it was joining,
  draining, or in maintenance when it crashed.
- **Routing correctness**: peers need a reliable, propagatable signal to decide
  whether to include a node in replica sets.
- **Operator visibility**: there is no authoritative source of truth about a
  node's current role.
- **Safe maintenance**: operators need to quiesce a node's KV traffic (causing
  load-balancer health checks to fail and traffic to reroute) without triggering
  an expensive drain.

The `MemberPhase` FSM, the `Member` gossip record, and `state.toml`
collectively form a durable, propagatable, and inspectable record of a node's
intent throughout its lifetime.

---

## CLI contract

### `tourctl node inspect`

Reads the target node's state via the `servers.peer` listener over mTLS.

**Healthy READY node:**

```
$ tourctl node inspect
╭──────────────────────────────────────────────────╮
│  Node         node-3                             │
│  Phase        READY                              │
│  Generation   2                                  │
│  Seq          47                                 │
│  Ring epoch   5                                  │
│  Partitions   80 owned / 240 total               │
│                                                  │
│  Listeners                                       │
│    KV     node-3.example.com:7700   UP           │
│    Peer   node-3.example.com:7701   UP           │
│                                                  │
│  Peers (3)                                       │
│    node-1   READY      gen=2  seq=51  REACHABLE  │
│    node-2   READY      gen=1  seq=38  REACHABLE  │
│    node-3   READY      gen=2  seq=47  —          │
╰──────────────────────────────────────────────────╯
```

**PAUSED node:**

```
$ tourctl node inspect
╭──────────────────────────────────────────────────╮
│  Node         node-3                   ⏸ PAUSED  │
│  Phase        PAUSED                             │
│  Paused from  DRAINING                           │
│  Generation   2                                  │
│  Ring epoch   5                                  │
│                                                  │
│  Listeners                                       │
│    KV     node-3.example.com:7700   PAUSED ✗     │
│    Peer   node-3.example.com:7701   UP     ✓     │
│                                                  │
│  Resume: tourctl node resume                     │
╰──────────────────────────────────────────────────╯
```

**FAILED node:**

```
$ tourctl node inspect
╭──────────────────────────────────────────────────╮
│  Node         node-3           ⚠  FAILED         │
│  Phase        FAILED                             │
│  Failed from  JOINING                            │
│  Last error   seed unreachable after 120s        │
│                                                  │
│  Recovery: tourctl node join [--seed host:port]  │
╰──────────────────────────────────────────────────╯
```

**JSON output:**

```
$ tourctl node inspect --json
{
  "node_id": "node-3",
  "phase": "READY",
  "generation": 2,
  "seq": 47,
  "ring_epoch": 5,
  "partitions_owned": 80,
  "failed_from": null,
  "paused_from": null,
  "listeners": {
    "kv":   {"address": "node-3.example.com:7700", "up": true},
    "peer": {"address": "node-3.example.com:7701", "up": true}
  }
}
```

---

## Design

### 3.1 MemberPhase states

`MemberPhase` is the self-declared operational state propagated to all peers
via gossip. It represents what the node intends to be doing.

| Phase | Description |
|-------|-------------|
| `IDLE` | Not participating. Initial state on first start; also reached after a successful drain. |
| `JOINING` | Contacting seeds, receiving ring assignment, receiving partition data. |
| `READY` | Fully operational. Owns partitions; serves reads, writes, and replication. |
| `PAUSED` | Maintenance mode. **Everything is suspended**: KV TCP listener is closed, gossip emission stopped, replication envelopes refused (both inbound and outbound), rebalance transfers halted, quorum reads rejected. The `servers.peer` TCP socket remains bound **only** to accept `tourctl node resume` and `tourctl node inspect`. Peers learn of the pause via the last emitted gossip before suspension; they remove the node from preference lists within O(log N) gossip rounds. |
| `DRAINING` | Transferring partition ownership to remaining nodes. Reads still served; primary writes refused (`TEMPORARY_UNAVAILABLE`). |
| `FAILED` | A deadline-bounded operation (join or drain) expired after exhausting retries. Inert; awaits operator recovery. |

### 3.2 Full FSM

```
                        tourctl node join
  IDLE ──────────────────────────────────────────► JOINING
   ▲                                                   │   ╲
   │                                            complete╲   ╲ tourctl node pause
   │                                                    ╲▼   ▼
   │                                                    READY ──────────────────────┐
   │                                                      │  ╲                      │
   │                                           tourctl     │   ╲ tourctl node pause  │
   │                                           node pause  │    ▼                   │
   │                                                  ┌────┤   PAUSED               │
   │                                                  │    │     │                  │
   │                            tourctl node leave ───┘    │   tourctl node resume  │
   │                                                        │     │                  │
   │                                           ┌────────────┘     ▼  (paused_from)  │
   │                                           │           ┌──► READY (if READY)    │
   │                                           ▼           │    JOINING (if JOINING)│
   │                                       DRAINING        │    DRAINING (if DRAIN) │
   │                                           │           │                        │
   │                     tourctl node pause ──►│           │                        │
   │                                           │                                    │
   │                                    drain complete                              │
   │                                           │                                    │
   └───────────────────────────────────────────┘                                    │
                          IDLE ◄──────────────────────────────────────────────────► │

  Any JOINING, READY, DRAINING → deadline exceeded → FAILED
                FAILED → tourctl node join/leave (matching failed_from) → resumes
```

Simplified linear view:

```
IDLE → JOINING → READY → DRAINING → IDLE
         ↕ pause    ↕ pause    ↕ pause
        PAUSED     PAUSED     PAUSED
         → FAILED when deadline expires (join or drain)
         FAILED → recovery → resumes
```

### 3.3 Transition table

| From | To | Trigger | Notes |
|------|----|---------|-------|
| `IDLE` | `JOINING` | `tourctl node join` | Increments `generation`; resets `seq=0`. |
| `JOINING` | `READY` | Internal (all partitions committed) | `seq` bumped. |
| `JOINING` | `PAUSED` | `tourctl node pause` | `paused_from=JOINING` stored. |
| `JOINING` | `FAILED` | Deadline expired | `failed_from=JOINING`. |
| `READY` | `DRAINING` | `tourctl node leave` | `seq` bumped. |
| `READY` | `PAUSED` | `tourctl node pause` | `paused_from=READY`. |
| `PAUSED` | prev phase | `tourctl node resume` | Returns to `paused_from`. `seq` bumped. |
| `DRAINING` | `IDLE` | Internal (drain complete) | Ring epoch advances. |
| `DRAINING` | `PAUSED` | `tourctl node pause` | `paused_from=DRAINING`. |
| `DRAINING` | `FAILED` | Deadline expired | `failed_from=DRAINING`. |
| `FAILED` | `JOINING` | `tourctl node join` | Only when `failed_from=JOINING`. |
| `FAILED` | `DRAINING` | `tourctl node leave` | Only when `failed_from=DRAINING`. |

### 3.4 Invalid command / phase combinations

| Command | Phase | Response |
|---------|-------|----------|
| `node join` | `JOINING` | `ALREADY_JOINING` |
| `node join` | `READY` | `INVALID_PHASE` |
| `node join` | `PAUSED` | `INVALID_PHASE` (use `resume` first or `leave`) |
| `node join` | `DRAINING` | `INVALID_PHASE` |
| `node join` | `FAILED`, `failed_from=DRAINING` | `INVALID_PHASE` (use `leave`) |
| `node leave` | `IDLE` | `INVALID_PHASE` |
| `node leave` | `JOINING` | `INVALID_PHASE` |
| `node leave` | `DRAINING` | `ALREADY_DRAINING` |
| `node leave` | `FAILED`, `failed_from=JOINING` | `INVALID_PHASE` (use `join`) |
| `node pause` | `IDLE` | `INVALID_PHASE` |
| `node pause` | `PAUSED` | `ALREADY_PAUSED` |
| `node pause` | `FAILED` | `INVALID_PHASE` |
| `node resume` | anything but `PAUSED` | `INVALID_PHASE` |

### 3.5 Phase guards on handlers

The KV TCP listener (`servers.kv`) is **only open** when `phase = READY`. In all
other phases the socket is either not bound yet, closed, or not accepting
connections. This is the authoritative rule; the table below shows the resulting
combination of operation-level guards.

| Phase | `kv.put` | `kv.get` | `kv.delete` | `replicate` (inbound) | `replicate` (outbound) | quorum read | gossip recv | rebalance | KV TCP | `node.*` cmds |
|-------|:--------:|:--------:|:-----------:|:---------------------:|:----------------------:|:-----------:|:-----------:|:---------:|:------:|:-------------:|
| `IDLE` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | closed | `join` only |
| `JOINING` | ✗ | ✗ | ✗ | ✓ | ✓ | ✗ | ✓ | ✓ | closed | `pause` only |
| `READY` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | **open** | `leave`, `pause` |
| `PAUSED` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | closed | `resume`, `leave`* |
| `DRAINING` | ✗(primary) | ✓† | ✗(primary) | ✓ | ✓ | ✓ | ✓ | ✓ | closed | `pause` only |
| `FAILED` | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | ✗ | closed | `join`/`leave`‡ |

`*` `tourctl node leave` from `PAUSED` transitions directly to `DRAINING`.
`†` `DRAINING` serves `kv.get` via the **peer** TCP listener (internal quorum-read path), not the KV listener.
`‡` Only the command matching `failed_from` is accepted.

**PAUSED in detail:** When a node enters `PAUSED`:
1. KV TCP listener: closed (no new or existing client connections).
2. Gossip emission: stopped (last emitted record remains the peers' view).
3. Inbound `replicate` envelopes: refused with `TEMPORARY_UNAVAILABLE`.
4. Outbound `replicate` sends: cancelled; pending hints remain queued.
5. Rebalance transfers (both join and drain variants): suspended; in-progress
   transfers fail; their retry/deadline clock is **frozen** (does not advance
   while `PAUSED`).
6. Peer TCP listener: remains bound, accepting only `node.resume` and
   `node.inspect` envelope kinds.

On `tourctl node resume`, step 1–6 are reversed in order (peer fully active
before gossip re-emitted before KV listener reopened), and suspended operations
resume from where they were interrupted.

### 3.6 Node state file (`state.toml`)

The state file is the single durable record of a node's lifecycle identity.
It is **strictly separate from `LocalStoragePort`** (the KV engine) and must
survive KV engine replacements, resets, and in-memory teardowns.

#### Location

`<data_dir>/state.toml` where `data_dir` comes from `[node].data_dir` in
`config.toml`. The directory may vary between restarts (e.g., a different
volume mount). The node **never** derives a path from the config file's own
directory. `data_dir` is created with mode `0700` if it does not exist.

#### Process lock

`<data_dir>/pid.lock` is acquired with an exclusive OS-level file lock before
any other file in `data_dir` is opened or created. This prevents two `tourillon`
processes from writing to the same state concurrently ("zombie" isolation):

- Lock acquired with `fcntl.lockf(LOCK_EX | LOCK_NB)` on POSIX.
- On Windows: `msvcrt.locking` or `LockFileEx`.
- A second process attempting to start with the same `data_dir` receives an
  immediate startup error (see proposal 001 — Error paths).
- The OS releases the lock automatically on process exit/crash; no manual
  clean-up is needed.

#### State write lock

All read-modify-write cycles on `state.toml` are protected by an
**asyncio-level lock** (`asyncio.Lock`) private to the running process. This
serialises concurrent write requests from different coroutines (gossip, join
handler, drain coordinator) within the same process. The sequence for every
state write is:

```
1. Acquire asyncio.Lock (async).
2. Write new TOML to <data_dir>/state.toml.tmp
3. os.fsync(<tmp-fd>)
4. os.replace(<tmp>, <data_dir>/state.toml)   ← atomic on POSIX & NTFS
5. Release asyncio.Lock.
6. (After lock released) Emit gossip if phase changed.
```

The OS-level process lock and the asyncio write lock serve different purposes
and are both required.

#### Format

```toml
schema_version = 1   # checked at read; unrecognised version → startup failure

[identity]
node_id    = "node-3"
generation = 2         # incremented exactly once per IDLE → JOINING

[lifecycle]
phase       = "READY"  # IDLE | JOINING | READY | PAUSED | DRAINING | FAILED
seq         = 47       # bumped on every gossip emission; reset when generation increments
failed_from = ""       # "JOINING" or "DRAINING" when phase=FAILED; empty otherwise
paused_from = ""       # "JOINING", "READY", or "DRAINING" when phase=PAUSED; empty otherwise

[ring]
epoch = 5

[seeds]
# Populated on first successful seed contact during a seeded join.
# Used automatically to re-contact the cluster on restart from JOINING or DRAINING.
last_known = ["node-1.example.com:7701", "node-2.example.com:7701"]

[pause]
# Written when entering PAUSED; used by resume to know what to restore.
deadline_remaining_s  = 0.0   # seconds remaining on join/drain deadline when pause began;
                               # 0.0 when paused_from=READY (no active deadline)
retries_remaining     = -1    # remaining retry budget at pause time;
                               # -1 means unlimited (max_retries=-1 was configured)
```

#### Concurrency and atomicity

Write ordering and locking are described in "State write lock" above.

A stale process lock (crashed process) is automatically released by the OS.
A stale `state.toml.tmp` left behind by a crash is silently discarded on
next startup.

#### First-startup initialisation

When no state file exists:

| Field | Initial value |
|-------|--------------|
| `identity.node_id` | `[node].id` from config |
| `identity.generation` | `1` |
| `lifecycle.phase` | `IDLE` |
| `lifecycle.seq` | `0` |
| `lifecycle.failed_from` | `""` |
| `lifecycle.paused_from` | `""` |
| `ring.epoch` | `0` |
| `seeds.last_known` | `[]` |

#### Identity cross-check on restart

```python
if config.node_id != state.node_id:
    raise StateIdentityMismatch(
        f"config node_id={config.node_id!r} "
        f"≠ state file node_id={state.node_id!r}"
    )
```

Prevents misrouting a config file to the wrong state file or renaming a node.

#### Write-before-gossip ordering

The state file is written and the lock released **before** the corresponding
gossip record is emitted. A crash after the file write but before gossip
recovers cleanly: the node restarts from the correct, more-advanced phase.
Reversing this order is a correctness violation.

### 3.7 Member value object

`Member` is the unit of gossip exchange.

| Field | Type | Gossiped? | Description |
|-------|------|:---------:|-------------|
| `node_id` | `str` | Yes | Stable logical identifier. |
| `peer_address` | `str` | Yes | mTLS peer endpoint (`host:port`). |
| `generation` | `int` | Yes | Re-entry counter; incremented once at `IDLE → JOINING`. |
| `seq` | `int` | Yes | Emission counter; bumped on every gossip emission. |
| `phase` | `MemberPhase` | Yes | Self-declared phase. `FAILED` is **never** gossiped. |

`PAUSED` **is** gossiped so that peers can immediately remove the node from
preference lists and stop routing KV traffic to it.

**Merge rule:**

```python
def supersedes(self, other: Member) -> bool:
    if self.generation != other.generation:
        return self.generation > other.generation
    return self.seq > other.seq
```

Higher `generation` wins unconditionally. Equal generation: higher `seq` wins.

**Generation and seq invariants:**
- `generation` incremented **exactly once**: at `IDLE → JOINING`. Never on
  restart, never on `FAILED → JOINING` retry.
- `seq` reset to 0 only when `generation` increments.
- `seq` incremented on every gossip emission (including restarts where the node
  re-announces its persistent phase).

### 3.8 Local reachability

Private, per-peer, never gossiped.

| State | Meaning |
|-------|---------|
| `REACHABLE` | Recent operations succeeded. |
| `SUSPECT` | One or more recent operations failed. |
| `DEAD` | Unresponsive long enough to activate hinted handoff and exclude from routing. |

Driven by operation outcomes only — no heartbeat timer. A failed `replicate`,
`handoff.push`, or probe raises suspicion. A successful operation lowers it.

### 3.9 Startup sequence

Strictly ordered; any failure aborts and releases prior resources.

| Step | Action | Abort condition |
|------|--------|-----------------|
| 1 | Config resolution (`load_config`) | Missing field, unknown `schema_version`, invalid value |
| 2 | `data_dir` creation (`mkdir -p`, mode `0700`) | Permission denied |
| 3 | Process lock acquisition (`<data_dir>/pid.lock`) | Lock already held by another process |
| 4 | TLS context construction (two `ssl.SSLContext`) | Cert expired, key mismatch |
| 5 | State file read or first-time create | `StateIdentityMismatch`, `StateFileError`, unknown `schema_version` |
| 6 | Storage initialisation | Backend unavailable |
| 7 | `servers.peer` listener binding | Address in use |
| 8 | `servers.kv` listener binding (only if phase will be `READY`) | Address in use, duplicate with peer address |
| 9 | (fresh start only) write `phase=IDLE` to state file | I/O error |
| 10 | Asyncio event loop entry | — |

**KV listener binding:** the KV TCP socket is bound only when the node will
immediately serve KV traffic (i.e., a fresh start with persisted `phase=READY`
or a resume from `PAUSED` where `paused_from=READY`). For all other phases the
KV socket is opened later, at the moment the node transitions to `READY`.

If persisted phase is `JOINING` or `DRAINING`: enter the resume path (proposals
002 and 004). If `READY` or `PAUSED`: re-announce with `seq+1` (KV socket
opened only for `READY`). If `FAILED`: bind peer listener only, enter inert
mode, log `failed_from`.

### 3.10 Restart semantics per phase

| Phase | Restart behaviour | KV TCP | `generation` | `seq` |
|-------|-------------------|:------:|:---:|:---:|
| `IDLE` | Stays IDLE. No gossip. Awaits `tourctl node join`. | closed | — | — |
| `JOINING` | Re-contacts via `last_known`; resumes transfer from last committed batch. Fresh join deadline begins. | closed | unchanged | +1 |
| `READY` | Re-announces READY. Serves traffic immediately. | **open** | unchanged | +1 |
| `PAUSED` | Re-enters PAUSED (everything suspended). Awaits `tourctl node resume`. `deadline_remaining_s` restored. | closed | unchanged | +1 |
| `DRAINING` | Resumes drain from checkpoint. Already-committed partitions skipped. Fresh drain deadline begins. | closed | unchanged | +1 |
| `FAILED` | Binds peer listener. Inert. Logs `failed_from` and last error. Awaits `tourctl`. | closed | unchanged | — |

---

## Design decisions

### Decision: `PAUSED` can interrupt `JOINING` and `DRAINING`

**Alternatives considered:** `PAUSED` only from `READY`; force completion of
join/drain before maintenance.
**Chosen because:** operators need to be able to intervene in a stalled join or
drain without waiting for the deadline. A join or drain that is progressing
slowly (e.g., bandwidth-constrained) should be pausable for emergency
maintenance. The `paused_from` field encodes exactly where to resume.

### Decision: `PAUSED` gossips (unlike `FAILED`)

**Alternatives considered:** silence during pause (same as `FAILED`).
**Chosen because:** before suspending gossip emission, the node emits one final
`PAUSED` gossip record. Peers receive this in O(log N) rounds and immediately
remove the node from preference lists, causing LB health checks to fail and
traffic to reroute. After that single emission, gossip is stopped for the
duration of the pause. This is faster than relying on silence + local
reachability timeouts (which take seconds to minutes), while still honouring
the "everything suspended" invariant during the pause plateau.

### Decision: join/drain deadline clock frozen while `PAUSED`

**Alternatives considered:** deadline continues to tick during pause, possibly
expiring to `FAILED` while paused.
**Chosen because:** an operator who pauses for emergency maintenance should not
find the node in `FAILED` on resume. The deadline is an operational budget, not
a wall-clock timer. Freezing it preserves the operator's intent. The
`deadline_remaining_s` field in `state.toml` records the remaining budget
at pause time so it can be restored exactly on resume or restart.

### Decision: separate `paused_from` from `failed_from`

**Alternatives considered:** reuse `failed_from` for both.
**Chosen because:** the semantics are orthogonal. `failed_from` records a
terminal failure for operator diagnosis. `paused_from` records a voluntary
suspension for seamless resume. Conflating them would obscure whether a node
is in an error state or a maintenance state.

### Decision: write-before-gossip as an absolute invariant

**Alternatives considered:** gossip-then-write for lower perceived latency;
concurrent.
**Chosen because:** gossip-first creates a crash window where peers believe a
phase the node itself does not hold after restart. The node would re-enter the
wrong phase, diverging from what peers expect. Write-first guarantees the node
always restarts from a state at least as advanced as the last gossip.

---

## Proposed code organisation

The following files do not yet exist.

```
tourillon/
  core/
    structure/
      membership.py    # MemberPhase (StrEnum), Member (dataclass + supersedes)
    ports/
      state.py         # NodeStatePort Protocol
    handlers/
      inspect.py       # tourctl node inspect response handler

  infra/
    state/
      toml.py          # TomlNodeStateAdapter (asyncio lock + temp-file atomic write)
      process_lock.py  # ProcessLock: fcntl/LockFileEx pid.lock acquisition

tourctl/
  core/
    commands/
      node_inspect.py  # `tourctl node inspect` CLI entry point
```

---

## Interfaces (informative)

```python
# tourillon/core/structure/membership.py
from enum import StrEnum
from dataclasses import dataclass


class MemberPhase(StrEnum):
    IDLE     = "IDLE"
    JOINING  = "JOINING"
    READY    = "READY"
    PAUSED   = "PAUSED"
    DRAINING = "DRAINING"
    FAILED   = "FAILED"


@dataclass(frozen=True)
class Member:
    """Unit of gossip exchange.

    FAILED is never emitted as a gossiped phase. PAUSED is gossiped so that
    peers immediately update preference lists and load balancers reroute.
    """
    node_id:      str
    peer_address: str
    generation:   int
    seq:          int
    phase:        MemberPhase

    def supersedes(self, other: "Member") -> bool:
        if self.generation != other.generation:
            return self.generation > other.generation
        return self.seq > other.seq


# tourillon/core/ports/state.py
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class NodeState:
    node_id:              str
    generation:           int
    phase:                MemberPhase
    seq:                  int
    failed_from:          str         # "" | "JOINING" | "DRAINING"
    paused_from:          str         # "" | "JOINING" | "READY" | "DRAINING"
    ring_epoch:           int
    last_known:           list[str] = field(default_factory=list)
    deadline_remaining_s: float = 0.0  # remaining budget frozen at pause time
    retries_remaining:    int = -1     # remaining retry count frozen at pause time; -1 = unlimited


class NodeStatePort(Protocol):
    """Read and atomically write the node state file.

    Implementations must:
    - Acquire the asyncio write lock before any read-modify-write.
    - Use temp-file + os.replace for atomic writes (fsync before rename).
    - Release the asyncio lock BEFORE emitting any gossip record.
    The OS-level process lock (pid.lock) is acquired once at startup and
    held for the process lifetime; it is not rechecked here.
    """

    async def read(self) -> NodeState: ...
    async def write(self, state: NodeState) -> None: ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | Fresh start, no state file | Start node | State file created; `phase=IDLE`; `generation=1` |
| 2 | `IDLE` node | `kv.put` | `TEMPORARY_UNAVAILABLE` |
| 3 | `READY` node | `kv.put` | Accepted; KV TCP listener open |
| 4 | `PAUSED` node | `kv.put` | `TEMPORARY_UNAVAILABLE` |
| 5 | `PAUSED` node | `replicate` (inbound) | `TEMPORARY_UNAVAILABLE` |
| 6 | `PAUSED` node | `kv.get` | `TEMPORARY_UNAVAILABLE` |
| 7 | `DRAINING` node | `kv.get` (via peer channel) | Accepted |
| 8 | `DRAINING` node | `kv.put` (primary) | `TEMPORARY_UNAVAILABLE` |
| 9 | `FAILED` node | `replicate` | `TEMPORARY_UNAVAILABLE` |
| 10 | `Member(gen=3, seq=1)` vs `Member(gen=2, seq=99)` | `supersedes()` | gen=3 wins |
| 11 | Node in `JOINING`; 60s deadline elapsed; pause issued | `tourctl node pause` | State written `PAUSED`, `deadline_remaining_s=60.0`; gossip emitted once then stopped |
| 12 | Node `PAUSED` with `deadline_remaining_s=60.0`; `tourctl node resume` | Resume | Deadline restored to 60s; replication + gossip restarted; join resumes |
| 13 | Same `data_dir`; second process started | Startup | Second process exits with `pid.lock` error immediately |
| 14 | Node `READY`; transition to `PAUSED` | `kv.get` | KV TCP closed; `TEMPORARY_UNAVAILABLE` |
| 15 | Node `PAUSED`; `tourctl node leave` issued | Command | Transition to `DRAINING` (not `PAUSED`); drain begins with fresh deadline |
| 10 | `Member(gen=2, seq=7)` vs `Member(gen=2, seq=5)` | `supersedes()` | seq=7 wins |
| 11 | Persisted `phase=READY`, `generation=3` | Stop + restart | `phase=READY`; `seq+1`; `generation=3` unchanged |
| 12 | Persisted `phase=PAUSED`, `paused_from=DRAINING` | Stop + restart | `phase=PAUSED`; KV refused; peer accepted; `paused_from=DRAINING` preserved |
| 13 | Persisted `phase=FAILED`, `failed_from=JOINING` | Stop + restart | Inert; log entry references `failed_from=JOINING`; awaits `tourctl` |
| 14 | Config `node_id=A`; state file `node_id=B` | Start node | `StateIdentityMismatch`; exit 1 |
| 15 | `READY` node | `tourctl node inspect` | Returns phase, gen, seq, epoch, listener status, peer list |
| 16 | Write-before-gossip invariant | Transition any phase via in-memory handler | Assert state file written before gossip record emitted (checked via mock gossip transport) |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] All phase guard rules from §3.5 verified by dedicated unit tests.
- [ ] Write-before-gossip invariant verified for every transition.
- [ ] `StateIdentityMismatch` raised when `node_id` mismatches.
- [ ] `PAUSED` node: KV returns `TEMPORARY_UNAVAILABLE`; `replicate` accepted.
- [ ] Restart from `PAUSED` does not resume automatically; preserves `paused_from`.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

Join protocol internals (proposal 002), leave/drain protocol (proposal 004),
pause/resume protocol (proposal 005), KV data plane (proposal 006).
