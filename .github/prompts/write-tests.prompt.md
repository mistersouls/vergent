---
mode: agent
description: Write pytest tests for a Tourillon module, grounded in the relevant proposal's test scenarios table.
---

# Write tests for a Tourillon module

Target module: **${input:modulePath}**
Relevant proposal: **${input:proposalFile}** (leave blank if unknown)

## What to do

1. **Read the proposal** (if given) and locate its "Test scenarios" table.
   Each numbered row in that table maps to one test function.

2. **Read the module under test** to understand its public API and
   any invariants expressed in docstrings.

3. **Write the test file** at the mirrored path under `tests/`
   (e.g. `tourillon/core/ring/ring.py` → `tests/core/ring/test_ring.py`).

## Structure rules

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Tests for <module summary>."""

import pytest

# Fixtures at top; tests below.
# One test function per scenario row.
# async def test_<number>_<what>_<condition>_<expected>() -> None:
```

## Mandatory patterns

- `asyncio_mode = "auto"` is already set — all async tests run automatically.
- Mark every test with the appropriate pytest mark:
  `@pytest.mark.bootstrap`, `@pytest.mark.ring`, `@pytest.mark.lifecycle`,
  `@pytest.mark.leave`, `@pytest.mark.pause`, `@pytest.mark.kv`.
- Use in-memory adapters (stubs/fakes) — never real sockets or disk in
  unit tests. Real-socket tests live under `tests/e2e/` and are tagged
  `@pytest.mark.e2e`.
- Use `pytest.raises(ExceptionClass, match="substring")` for error cases.
- No `unittest.TestCase`. Type-annotate everything, including `-> None`.
- Cover: happy path, every error path listed in the proposal, and boundary
  conditions (e.g. `kind_len == 0`, `kind_len == KIND_MAX_LEN`,
  `kind_len == KIND_MAX_LEN + 1`).

## Invariants to probe

For **ring** tests: verify immutability — `add_vnodes` / `drop_nodes` must
return a new instance and leave the original unchanged.

For **envelope** tests: round-trip encode → decode must preserve all fields.

For **phase** tests: forbidden transitions must raise or be rejected;
`generation` must not change on retry or restart.

For **kv** tests: concurrent writes on the same key must converge to the
highest-HLC version; a tombstone must win over any older version.
