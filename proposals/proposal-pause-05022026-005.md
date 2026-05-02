# Proposal: Node Pause and Resume (`tourctl node pause` / `tourctl node resume`)

**Author**: Souleymane BA <soulsmister@gmail.com>
**Status:** Draft
**Date:** 2026-05-02
**Sequence:** 005
**Schema version:** 1

---

## Summary

This proposal specifies the `PAUSED` maintenance state: the conditions under
which a node enters and exits it, the complete suspension semantics (what stops,
what remains available), the interaction with ongoing join and drain deadlines,
the restart behaviour from `PAUSED`, and the `tourctl node pause` /
`tourctl node resume` CLI commands.
The `PAUSED` state itself is defined in proposal 003 (§3.1–3.5); this proposal
is the authoritative reference for the implementation of the two commands and
all suspension/resume logic.

---

## Motivation

Operators need to temporarily quiesce a node for maintenance (kernel upgrades,
disk replacement, network maintenance windows) without triggering an expensive
drain. The requirements are:

1. **Immediate traffic relief**: load balancers and write coordinators must stop
   routing to the node within seconds of the pause command.
2. **Clean suspension**: no partial writes, no mid-flight rebalance left in an
   indeterminate state.
3. **Preserved budget**: if a join or drain was in progress, its deadline should
   not tick away during maintenance.
4. **Simple recovery**: a single `tourctl node resume` command puts the node back
   exactly where it was.
5. **Crash-safe**: if the node crashes while paused, it restarts still paused,
   with the correct deadline remaining.

---

## CLI contract

### `tourctl node pause`

Connects to `servers.peer` over mTLS and triggers the pause protocol.

**Pause a READY node:**

```
$ tourctl node pause
Connecting to node-3.example.com:7701 ...
✓ node-3 is PAUSED
  KV listener:  closed
  Gossip:       suspended (last record emitted)
  Replication:  suspended
  Resume with:  tourctl node resume
```

**Pause a JOINING node (mid-join maintenance):**

```
$ tourctl node pause
Connecting to node-3.example.com:7701 ...
  node-3 is JOINING (generation 1, 40/80 partitions committed)
  Suspending join — deadline frozen at 73.4s remaining
✓ node-3 is PAUSED  (paused_from=JOINING)
  Resume with:  tourctl node resume
```

**Pause a DRAINING node:**

```
$ tourctl node pause
Connecting to node-3.example.com:7701 ...
  node-3 is DRAINING (60/80 partitions committed)
  Suspending drain — deadline frozen at 218.7s remaining
✓ node-3 is PAUSED  (paused_from=DRAINING)
  Resume with:  tourctl node resume
```

**Already paused:**

```
$ tourctl node pause
✗ node-3 is already PAUSED.
  Monitor with `tourctl node inspect`.  exit code 1
```

**Invalid phases:**

```
$ tourctl node pause   # IDLE
✗ node-3 is IDLE — cannot pause a node that has not joined.
  Use `tourctl node join` first.  exit code 1

$ tourctl node pause   # FAILED
✗ node-3 is FAILED — cannot pause a failed node.
  exit code 1
```

### `tourctl node resume`

Connects to `servers.peer` over mTLS and triggers the resume protocol.
The node returns to exactly the phase stored in `paused_from`.

**Resume to READY:**

```
$ tourctl node resume
Connecting to node-3.example.com:7701 ...
  Restoring READY state ...
  Reopening KV listener …
  Restarting gossip …
✓ node-3 is READY
  KV listener:  node-3.example.com:7700  UP
```

**Resume to JOINING:**

```
$ tourctl node resume
Connecting to node-3.example.com:7701 ...
  Restoring JOINING state (budget: 73.4s remaining) ...
  Restarting replication handlers …
  Resuming partition transfer (40/80 committed) …
✓ node-3 is JOINING (resumed)
```

**Resume to DRAINING:**

```
$ tourctl node resume
Connecting to node-3.example.com:7701 ...
  Restoring DRAINING state (budget: 218.7s remaining) ...
  Resuming drain from partition 61 …
✓ node-3 is DRAINING (resumed)
```

**Not paused:**

```
$ tourctl node resume   # READY
✗ node-3 is READY — only a PAUSED node can be resumed.  exit code 1
```

---

## Design

### 5.1 `* → PAUSED` transition

Applicable from: `JOINING`, `READY`, `DRAINING`.

```
1. Record remaining budget:
     deadline_remaining  = (deadline_start + deadline_duration) - now()
     # 0.0 when paused_from = READY (no active deadline)
     retries_remaining   = current_retries_remaining
     # -1 when max_retries = -1 (unlimited)

2. Acquire asyncio write lock.
3. Write to state.toml:
     phase                = PAUSED
     paused_from          = <current phase>
     deadline_remaining_s = deadline_remaining
     retries_remaining    = retries_remaining
     seq                  = seq + 1
4. Release asyncio write lock.

5. Emit one final gossip record: phase=PAUSED, seq=new_seq.
   (This is the ONLY gossip emitted while PAUSED.)

6. Suspend operations in this order:
   a. Cancel in-progress rebalance transfers (join or drain).
      Committed partitions already in the transfer log are preserved.
      In-flight batches are abandoned; they will be retried on resume.
   b. Cancel outbound replication tasks; drain the outbound queue gracefully
      (give in-flight sends at most `attempt_timeout` to complete).
   c. Stop accepting inbound `replicate` envelopes (handler returns
      TEMPORARY_UNAVAILABLE immediately).
   d. Stop gossip emission loop.
   e. Close KV TCP listener (if open — only in READY).

7. Node enters maintenance plateau:
   - Peer TCP listener accepts ONLY `node.resume` and `node.inspect` kinds.
   - All other envelope kinds receive TEMPORARY_UNAVAILABLE.
```

State file write (step 3) happens **before** gossip emission (step 5) which
happens **before** suspension (step 6). A crash after step 3 but before step 6
restarts cleanly as `PAUSED` (correct state already on disk).

### 5.2 `PAUSED → *` transition (resume)

```
1. Acquire asyncio write lock.
2. Read state file → paused_from, deadline_remaining_s.
3. Write to state.toml:
     phase       = paused_from
     paused_from = ""
     seq         = seq + 1
     # deadline_remaining_s left in place; cleared by join/drain completion
4. Release asyncio write lock.

5. Restore operations in reverse suspension order:
   a. Restart gossip emission loop.
   b. Start accepting inbound `replicate` envelopes.
   c. Restart outbound replication (deliver any pending hints using original HLC).
   d. If paused_from = READY: open KV TCP listener.
   e. If paused_from = JOINING: re-enter join loop with
        deadline  = deadline_remaining_s
        retries   = retries_remaining   # -1 if originally unlimited
   f. If paused_from = DRAINING: re-enter drain loop with
        deadline  = deadline_remaining_s
        retries   = retries_remaining

6. Emit gossip: phase=paused_from, seq=new_seq.
```

Gossip emission (step 6) happens **after** all listeners and handlers are
restored (step 5), so peers learn the node is active only once it is actually
ready to serve.

### 5.3 Deadline and retry freeze invariants

**Deadline:** when `PAUSED` is written to the state file, the join or drain
deadline clock stops advancing. `deadline_remaining_s` records the exact
remaining budget as a float (seconds). On resume:
`new_deadline_end = now() + deadline_remaining_s`.
On restart from `PAUSED`, the same value is read from the state file.
If `paused_from = READY`, `deadline_remaining_s = 0.0` (no deadline active).

**Retry count:** when `PAUSED` is written, the current remaining retry count
is also frozen into `retries_remaining`. On resume, the join or drain loop
initialises its counter from this value rather than from the config default.
`retries_remaining = -1` means unlimited (config had `max_retries = -1`).
This prevents an operator from inadvertently resetting an exhausted retry
budget by simply pausing and resuming.

### 5.4 Interaction with hinted handoff

Pending hints (writes queued for a `DEAD` peer) are **not delivered** while
`PAUSED`. The hint queue is preserved in memory and on disk. On resume, hint
delivery restarts from the head of the queue to the now-`REACHABLE` peers.
Hints are never re-timestamped; their original HLC is preserved (see proposal
006).

### 5.5 Concurrent pause requests

If two `tourctl node pause` commands arrive concurrently (e.g., two operator
terminals):
- The first one acquires the asyncio write lock and writes `PAUSED`.
- The second one reads `phase=PAUSED` before or after the write and returns
  `ALREADY_PAUSED` (exit code 1).

This is safe because the asyncio write lock serialises state writes within the
process.

---

## Core invariants

- `PAUSED` can only be entered from `JOINING`, `READY`, or `DRAINING`.
- `PAUSED` can only be exited via `tourctl node resume` (or kill + restart,
  which also re-enters `PAUSED` from the state file).
- The state file is written (`phase=PAUSED`) **before** any gossip is emitted.
- Exactly **one** gossip record is emitted during the entire `PAUSED` plateau.
- No inbound `replicate` envelopes are processed while `PAUSED`.
- No outbound `replicate` sends are initiated while `PAUSED`.
- No rebalance (join transfer or drain transfer) progresses while `PAUSED`.
- The KV TCP listener is closed for the entire `PAUSED` duration.
- The join/drain deadline clock does **not** advance while `PAUSED`.
- The retry counter does **not** advance while `PAUSED`.
- `deadline_remaining_s` and `retries_remaining` in the state file are the
  canonical records of the frozen budget; `deadline_remaining_s` is never
  negative; `retries_remaining` is `-1` iff `max_retries` was configured as `-1`.

---

## Design decisions

### Decision: everything suspended in PAUSED (including replication)

**Alternatives considered:** pause only KV client traffic; continue gossip and
replication in the background.
**Chosen because:** partial suspension is complex to reason about and to test.
If replication continues while paused, the node can receive new versions that
diverge from what the operator believes is frozen. A complete suspension makes
the node's state predictable: no write arrives, no write departs, no deadline
ticks. The suspended node is a clean snapshot of its state at pause time.

### Decision: single gossip emission on enter; zero on the plateau

**Alternatives considered:** emit gossip periodically during pause; stop all
gossip on pause (including the initial PAUSED record).
**Chosen because:** the single PAUSED record is necessary for peers to quickly
reroute traffic (O(log N) rounds). After that, silence is correct — the PAUSED
state is stable until resume, and peers do not need further updates. Continued
gossip would force keeping the gossip loop active, which contradicts the "fully
suspended" invariant.

### Decision: deadline frozen, not reset, on resume

**Alternatives considered:** give the node a fresh full deadline on resume;
deduct only real-time elapsed from the deadline.
**Chosen because:** a fresh deadline on resume would let an operator extend the
deadline indefinitely by pausing and resuming repeatedly. Deducting real-time
elapsed (i.e., not freezing) would punish legitimate maintenance windows. The
frozen budget is both fair and predictable: if 73 seconds remain when paused,
exactly 73 seconds remain on resume.

### Decision: retry count also frozen (not just deadline)

**Alternatives considered:** reset retry count to `max_retries` on resume;
not tracking retry count across pause.
**Chosen because:** an operator who configured `max_retries = 3` to limit blast
radius should not have that limit silently bypassed by pausing and resuming.
Freezing `retries_remaining` alongside `deadline_remaining_s` ensures both
dimensions of the budget are faithfully preserved. The state file is the single
source of truth; both values survive crashes and restarts.

### Decision: peer listener remains bound in PAUSED, accepting only resume/inspect

**Alternatives considered:** close the peer listener entirely; keep peer fully
operational.
**Chosen because:** closing the peer listener makes the node completely
unreachable — an operator cannot issue `tourctl node resume` remotely. Keeping
it fully operational contradicts the "fully suspended" invariant. The middle
ground — bound but restricted to two envelope kinds — preserves remote
recoverability without leaking any KV or replication semantics.

---

## Proposed code organisation

```
tourillon/
  core/
    handlers/
      pause.py           # PauseHandler: tourctl node pause request processing
      resume.py          # ResumeHandler: tourctl node resume request processing
    structure/
      pause.py           # PauseCheckpoint dataclass (paused_from, deadline_remaining_s)

tourctl/
  core/
    commands/
      node_pause.py      # `tourctl node pause` CLI entry point
      node_resume.py     # `tourctl node resume` CLI entry point
```

---

## Interfaces (informative)

```python
# tourillon/core/structure/pause.py
from dataclasses import dataclass


@dataclass(frozen=True)
class PauseCheckpoint:
    """Snapshot of the paused node's resume context.

    Stored in state.toml [pause] section.
    deadline_remaining_s is 0.0 when paused_from=READY (no active deadline).
    retries_remaining is -1 when max_retries was configured as -1 (unlimited).
    """
    paused_from:          str    # "JOINING" | "READY" | "DRAINING"
    deadline_remaining_s: float  # seconds remaining on join/drain deadline at pause time
    retries_remaining:    int    # remaining retry budget at pause time; -1 = unlimited


# tourillon/core/handlers/pause.py
from typing import Protocol
from tourillon.core.ports.state import NodeStatePort
from tourillon.core.ports.gossip import GossipPort


class PauseHandler:
    """Handle a tourctl node pause command.

    Writes PAUSED to the state file, emits one gossip record, then
    suspends all replication, rebalance, and KV traffic.
    """

    async def handle(
        self,
        state: NodeStatePort,
        gossip: GossipPort,
    ) -> None: ...


class ResumeHandler:
    """Handle a tourctl node resume command.

    Restores the previous phase (paused_from), reopens the KV listener if
    READY, restarts gossip, and re-enters the join or drain loop with
    the frozen deadline.
    """

    async def handle(
        self,
        state: NodeStatePort,
        gossip: GossipPort,
    ) -> None: ...
```

---

## Test scenarios

| # | Fixture | Action | Expected |
|---|---------|--------|----------|
| 1 | `READY` node | `tourctl node pause` | State: `PAUSED`, `paused_from=READY`, `deadline_remaining_s=0.0`; one gossip emitted; KV TCP closed |
| 2 | `PAUSED` node (`paused_from=READY`) | `kv.put` | `TEMPORARY_UNAVAILABLE` |
| 3 | `PAUSED` node | `replicate` (inbound) | `TEMPORARY_UNAVAILABLE` |
| 4 | `PAUSED` node | `node.inspect` | Returns current state (no error) |
| 5 | `PAUSED` node | `tourctl node resume` | Phase restored to `READY`; KV TCP open; gossip restarted |
| 6 | `JOINING` node (40/80 partitions committed, 73s remaining) | `tourctl node pause` | `PAUSED`, `paused_from=JOINING`, `deadline_remaining_s=73.x`; transfer cancelled |
| 7 | `PAUSED` node (`paused_from=JOINING`, `deadline_remaining_s=73.0`) | `tourctl node resume` | Re-enters `JOINING`; deadline = 73.0s; skips 0–39; resumes 40–79 |
| 8 | `PAUSED` node; crash and restart | Restart | Phase still `PAUSED`; `deadline_remaining_s` unchanged from state file |
| 9 | `DRAINING` node (218s remaining) | `tourctl node pause` → crash → restart → `tourctl node resume` | On resume, drain continues with ≤218s budget |
| 10 | `PAUSED` node | `tourctl node pause` (second) | `ALREADY_PAUSED`; exit code 1 |
| 11 | `IDLE` node | `tourctl node pause` | `INVALID_PHASE`; exit code 1 |
| 12 | `FAILED` node | `tourctl node pause` | `INVALID_PHASE`; exit code 1 |
| 13 | `PAUSED` node | `tourctl node leave` | Transitions to `DRAINING`; fresh `drain.deadline`; paused_from cleared |
| 14 | Hint queued for peer C; node enters `PAUSED` | Check hint delivery | Hints not delivered while `PAUSED`; delivery resumes after `tourctl node resume` |
| 15 | `PAUSED` node; `join.max_retries=3`; resume | Resume | Remaining max_retries budget preserved (not reset to 3) |

---

## Exit criteria

- [ ] All test scenarios pass.
- [ ] `READY` → `pause` → `resume` → `READY` in a single terminal session; KV traffic interrupted only during pause.
- [ ] `JOINING` → `pause` (73s remaining) → restart → `resume` → `READY`; deadline ≤73s on resume.
- [ ] `DRAINING` → `pause` → `resume` → `IDLE`; no partition re-transferred.
- [ ] Peer node observes `PAUSED` in its member table within 2 gossip rounds; reverts after `resume`.
- [ ] `uv run pytest --cov-fail-under=90` passes.
- [ ] `uv run pre-commit run --all-files` passes.

---

## Out of scope

`tourctl node leave` from `PAUSED` (specified in proposal 004 §4.1), certificate
rotation, ring inspect.
