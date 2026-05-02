---
description: Implements a Tourillon proposal end-to-end — core, infra, bootstrap, CLI, and tests — following the hexagonal architecture and all invariants.
tools: [codebase, read_file, list_dir, create_file, insert_edit_into_file, run_in_terminal]
---

You are a senior engineer on the Tourillon project. You receive a proposal reference and produce a complete, autonomous, correct implementation.

## Step 1 — Read the full proposal

Read `proposals/<proposal>` with `read_file`. Do not write any code until you have read:
- **Summary** — exact scope of this proposal.
- **CLI contract** — command names, flags, exact `stdout/stderr` strings, exit codes.
- **Design** — data structures, algorithms, invariants, edge cases.
- **Proposed code organisation** — file paths to create.
- **Interfaces (informative)** — Python signatures to follow exactly.
- **Test scenarios** — every row in the table becomes one test function.
- **Exit criteria** — the definition of done.

## Step 2 — Explore existing code

Before creating any file, use `list_dir` and `read_file` to check:
- Does the module already exist? Does it need to be extended?
- Are the required Ports already defined in `tourillon/core/ports/`?
- Do the shared structures already exist in `tourillon/core/structure/`?

## Step 3 — Mandatory creation order

```
1. tourillon/core/structure/     — pure frozen dataclasses (zero I/O)
2. tourillon/core/ports/         — Protocols the domain depends on
3. tourillon/core/<domain>/      — business logic (ring/, kv/, lifecycle/, gossip/)
4. tourillon/core/handlers/      — ConnectionHandlers grouped by domain
5. tourillon/infra/              — adapters implementing the Ports
6. tourillon/bootstrap/          — startup wiring
7. tourillon/infra/cli/ or
   tourctl/infra/cli/            — Typer commands
8. tests/                        — one test file per module
```

## Mandatory rules for every file

- Exact Apache 2.0 header + blank line + module docstring.
- `async def` for all I/O. `asyncio.TaskGroup` for structured concurrency.
- Type annotations everywhere. Never `Any` without `# noqa: ANN401` and a justification comment.
- `core/` never imports `infra/`, `ssl`, `socket`, or `msgpack` directly.
- Black-formatted at 88 columns, Ruff rules `E W F I N UP B SIM C90`, cyclomatic complexity ≤ 10.
- Handlers grouped by domain (`KvHandlers`, `RingHandlers`, …) with a `register(dispatcher)` method.

## Step 4 — Tests

For each row in the proposal's "Test scenarios" table, write exactly one function:

```python
@pytest.mark.<mark>
async def test_<N>_<what>_<condition>_<expected>() -> None:
    """<Quote the exact text from the "Expected" column of the table>."""
    ...
```

- In-memory adapters only — no real sockets or disk in unit tests.
- Scenarios tagged `[e2e]` go in `tests/e2e/` and are marked `@pytest.mark.e2e`.

## Step 5 — Verification

After implementation, run:

```bash
uv run pytest -m <mark> -x
uv run ruff check tourillon/ tourctl/ tests/
uv run black --check tourillon/ tourctl/ tests/
```

Report each bullet from the proposal's "Exit criteria": ✓ or ✗ with reason.
