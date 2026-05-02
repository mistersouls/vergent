---
mode: agent
description: Implement a KV data-plane operation (put, get, delete) following proposal 006.
---

# Implement a KV data-plane operation

Operation: **`${input:operation}`** (`put`, `get`, or `delete`)

## Mandatory pre-reading

Read **`proposals/proposal-kv-dataplane-05022026-006.md`** in full. Pay
particular attention to:
- **Storage model** — `StoreKey`, `Version`, `Tombstone`, HLC semantics.
- **Coordinator path** — the node that receives the client request.
- **Replication fan-out** — how many replicas, which envelope kind.
- **Quorum** — `⌊rf/2⌋ + 1` acks required before responding to client.
- **Proxy path** — if this node is not the primary, it proxies.
- **Hinted handoff** — what happens when a replica is unreachable.
- **Convergence** — idempotent apply, HLC ordering, tombstone semantics.
- **CLI contract** — exact output strings and exit codes for
  `tourctl kv ${input:operation}`.

## Layers to implement

### 1. `tourillon/core/kv/${input:operation}.py` — domain logic

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Domain logic for the kv.${input:operation} operation."""
```

- Pure async function / class; no `ssl`, no sockets, no `msgpack`.
- Depends only on `StoragePort`, `ClockPort`, `ReplicationPort` (via ports).
- Enforces phase guard: `phase must be READY` (and `DRAINING` for `get`).
- HLC is advanced **once** per write operation at coordinator time.
- For `put`/`delete`: fan out to `rf` replicas, await quorum acks, then
  respond. Unreachable replicas → store hint via `HintedHandoffPort`.
- For `get`: read from local store; if not owned, proxy to primary.

### 2. `tourillon/core/handlers/kv_${input:operation}.py` — ConnectionHandler

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Handler for the `kv.${input:operation}` envelope kind."""
```

- Implements `ConnectionHandler` protocol.
- Deserialises payload via `SerializerPort`.
- Calls the domain function from layer 1.
- Sends response envelope: `kv.${input:operation}.ok` or `error.*`.

### 3. `tourctl/core/commands/kv_${input:operation}.py` — tourctl command

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""tourctl kv ${input:operation} domain function."""
```

- Builds the request `Envelope`, calls `TcpClient.request()`.
- Returns parsed domain result; raises on error envelopes.

### 4. `tourctl/infra/cli/kv.py` — Typer sub-command (add to existing)

- Flags: `--keyspace`, `--key`, and for `put`: `--value` / `--stdin`.
- Prints exactly the strings from the proposal's CLI contract.
- Exit codes: 0 success, 1 error, 2 not-found / quorum failure.

## Key invariants to enforce

- HLC `(wall_ms, logical, node_id)` — `logical` increments when
  `wall_ms == last_wall_ms`; resets to 0 on clock advance.
- `put` with an older HLC than the stored version is silently dropped
  (idempotent apply).
- `delete` writes a `Tombstone`; the tombstone wins over any `Version`
  with a strictly lower HLC.
- Two concurrent writes with the same HLC wall+logical: the one with the
  lexicographically greater `node_id` wins (deterministic tiebreak).
- `get` on a `Tombstone` key returns exit code `2` with a tombstone message,
  not a "key not found" message.

## Tests to write

In `tests/core/kv/test_${input:operation}.py`:

```python
@pytest.mark.kv
async def test_${input:operation}_happy_path_...) -> None: ...

@pytest.mark.kv
async def test_${input:operation}_quorum_not_met_...) -> None: ...

@pytest.mark.kv
async def test_${input:operation}_stale_write_dropped_...) -> None: ...
# (for put: older HLC ignored)

@pytest.mark.kv
async def test_${input:operation}_phase_guard_...) -> None: ...
# wrong phase → error.not_ready

@pytest.mark.kv
async def test_${input:operation}_concurrent_writes_converge_...) -> None: ...
# (for put/delete: highest HLC wins; tiebreak on node_id)
```

Use `FakeStoragePort`, `FakeClockPort`, `FakeReplicationPort` from
`tests/fakes/`. No real sockets.
