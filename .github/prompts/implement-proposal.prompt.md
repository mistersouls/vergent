---
mode: agent
description: Implement a Tourillon proposal end-to-end (core + infra + tests).
---

# Implement a Tourillon proposal

You are implementing **proposal ${input:proposalFile}** from `proposals/`.

## Step 1 — Read the proposal

Read the full proposal file now. Pay attention to:
- **Summary** — the boundary of this proposal.
- **CLI contract** — exact command names, flags, output strings.
- **Design** — data structures, algorithms, invariants.
- **Proposed code organisation** — file paths to create.
- **Interfaces (informative)** — Python signatures to follow exactly.
- **Test scenarios** — table of numbered scenarios; each one becomes a test.
- **Exit criteria** — the definition of done.

Do not proceed until you have read the entire proposal.

## Step 2 — Plan the implementation in order

Follow the hexagonal architecture:

```
1. core/structure/   — pure frozen dataclasses (no I/O, no third-party)
2. core/ports/       — Protocol interfaces the core depends on
3. core/ring/ (or core/kv/ etc.) — domain logic using only ports
4. core/handlers/    — ConnectionHandler implementations
5. infra/            — adapters implementing the ports
6. infra/cli/        — Typer commands wiring everything together
7. tests/            — one test file per module, all scenarios from the table
```

## Step 3 — Mandatory rules for every file

- Apache 2.0 header + blank line + module docstring.
- `async def` for all I/O. `asyncio.TaskGroup` for structured concurrency.
- Type-annotate everything. No bare `Any` without `# noqa: ANN401`.
- `core/` never imports `infra/` or third-party libraries (except via ports).
- `msgpack` only through `SerializerPort`; `ssl`/sockets only in `infra/`.
- Black-formatted, Ruff-clean (rule sets E W F I N UP B SIM C90).
- Pytest marks matching the proposal area (e.g. `@pytest.mark.ring`).

## Step 4 — Tests

For each row in the proposal's "Test scenarios" table, write exactly one
`async def test_<scenario_number>_<short_description>() -> None` function.
Use in-memory adapters for storage and transport in unit tests.
Tag `[e2e]` scenarios with `@pytest.mark.e2e` and skip them by default.

## Step 5 — Exit criteria

After implementation, verify every bullet in the proposal's "Exit criteria"
section. Report which ones pass and which ones still need work.
