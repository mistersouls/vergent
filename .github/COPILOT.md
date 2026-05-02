# GitHub Copilot — Usage Guide

This directory configures GitHub Copilot for the Tourillon project.
The setup consists of three layers that work together.

---

## How it works

```
.github/
  copilot-instructions.md   ← always-on repository context (loaded automatically)
  agents/                   ← autonomous agents for multi-step tasks
    invariant-auditor.md
    proposal-implementer.md
    test-writer.md
  prompts/                  ← reusable guided templates (invoked with #)
    implement-proposal.prompt.md
    new-envelope-handler.prompt.md
    new-cli-command.prompt.md
    add-port-adapter.prompt.md
    ring-operations.prompt.md
    kv-operation.prompt.md
    write-tests.prompt.md
```

---

## Quick reference

| Need | What to type |
|------|-------------|
| General question about the codebase | Ask directly — instructions are always loaded |
| Implement a full proposal autonomously | `@proposal-implementer` |
| Audit files for invariant violations | `@invariant-auditor` + attach files |
| Fill missing test coverage | `@test-writer` + attach the module |
| Step-by-step proposal implementation | `#implement-proposal` |
| Add a new envelope kind handler | `#new-envelope-handler` |
| Add a new CLI command | `#new-cli-command` |
| Add a port + infra adapter | `#add-port-adapter` |
| Implement a KV operation | `#kv-operation` |
| Work on ring logic | `#ring-operations` |
| Write tests for a module | `#write-tests` |

---

## Agents

Agents run in **autonomous mode**: they chain multiple tool calls (read files,
create files, run commands) without you driving each step. Invoke them with
`@<agent-name>` in Copilot Chat.

### `@invariant-auditor`

Reads the relevant proposals, scans the provided files, and reports every
invariant violation without modifying any code. Use before every pull request.

```
@invariant-auditor

[attach tourillon/core/ring/ring.py]
```

Expected output:
```
## Audit report — tourillon/core/ring/ring.py

### ✗ Violation: Immutable Ring
File   : tourillon/core/ring/ring.py:47
Code   : `self._vnodes.append(vnode)`
Rule   : Ring mutations must return new instances; never mutate in place.
Proposal: proposal-node-join-05022026-002.md §2.3

---
✓ No further violations detected.
```

### `@proposal-implementer`

Reads a proposal end-to-end, creates all files in hexagonal order
(structure → ports → domain → handlers → infra → bootstrap → CLI → tests),
runs the linter and test suite, and reports against the exit criteria.

```
@proposal-implementer

Implement proposals/proposal-node-lifecycle-05022026-003.md
```

### `@test-writer`

Reads the "Test scenarios" table in the relevant proposal, finds which
scenarios are already covered, and generates only the missing test functions.
Produces a coverage report at the end.

```
@test-writer

[attach tourillon/core/ring/ring.py]
```

---

## Prompts

Prompts are guided templates invoked with `#`. Copilot asks for the
`${input:...}` variables and produces a focused response for that single task.

### `#implement-proposal`

```
#implement-proposal

proposalFile: proposals/proposal-node-join-05022026-002.md
```

### `#new-envelope-handler`

```
#new-envelope-handler

kind: kv.put
domain: kv
handlerFileName: kv
handlerClassName: KvHandlers
verb: put
```

### `#new-cli-command`

```
#new-cli-command

command: tourctl kv get
binary: tourctl
commandModule: kv_get
verb: get
```

### `#add-port-adapter`

```
#add-port-adapter

portName: StoragePort
portModule: storage
adapterModule: store
adapterClass: RocksDbStorageAdapter
```

---

## Typical workflow for a new proposal

```
1. @proposal-implementer   →  creates all code and tests
2. @invariant-auditor      →  flags any violations in the generated files
3. @test-writer            →  fills any gaps in test coverage
4. uv run pytest -m <mark> →  green
5. uv run pre-commit run --all-files  →  green
```

---

## Always-on rules (copilot-instructions.md)

The following are enforced automatically on every Copilot suggestion — no need
to repeat them in prompts:

- Python 3.14 only. `async def` for all I/O. `asyncio.TaskGroup` for concurrency.
- Apache 2.0 header on every `.py` file.
- `core/` never imports `infra/`, `ssl`, `socket`, or `msgpack` directly.
- `Ring` mutations return new instances — never mutate in place.
- Phase guards: writes require `READY`; reads allow `READY | DRAINING`.
- `state.toml` is written **before** gossip is emitted on any phase transition.
- `Member.generation` increments exactly once per `IDLE → JOINING` transition.
- All TLS material is inline base64 PEM — no `*_file` path variants.
- `logging` for all observability — never `print()`.
