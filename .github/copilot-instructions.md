# GitHub Copilot Instructions — Tourillon

## Project identity

Tourillon is a **leaderless, peer-to-peer distributed key-value database**
written in Python 3.14. It guarantees deterministic convergence, ordered
hinted handoff, consistent-hashing ring partitioning, and mandatory mTLS for
all traffic.

---

## Language and runtime

- Target **Python 3.14** exclusively. Never suggest syntax or stdlib APIs that
  require a lower minimum or that are deprecated/removed in 3.14.
- Use modern Python idioms: `X | Y` union types, `match` statements,
  `TypeAlias`, `Self`, `ParamSpec`, `TypeVarTuple`, `@override`, etc.
- All public functions, methods, and class attributes **must** carry PEP 484
  type annotations. Never omit `-> None` on procedures.

---

## File header — mandatory

Every `.py` file **must** start with this exact Apache 2.0 header block,
followed by a blank line and then the module docstring:

```python
# Copyright 2026 Tourillon Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""<one-line module summary>."""
```

Always add this header when creating or suggesting a new `.py` file.

---

## Code style

- **Formatter:** Black, line-length 88. Emit code that Black would not
  reformat.
- **Linter:** Ruff with rule sets `E W F I N UP B SIM`. Fix all violations
  before suggesting code.
- **Imports:** one import per line; stdlib → third-party → first-party
  (`tourillon`), separated by blank lines (isort/ruff-I order).
- **Docstrings:** PEP 257. One-line summary ending with `.`; blank line before
  extended description; use imperative mood (`Return …`, `Raise …`).
- No wildcard imports (`from x import *`). No unused imports or variables.

---

## Architecture constraints

When generating or modifying implementation code, respect these invariants:

| Invariant | Rule |
|-----------|------|
| Leaderless | Never introduce a single-node coordination point or leader election. |
| Deterministic ordering | Per-key updates must use the project's ordering metadata — never `time.time()` or random tiebreaks. |
| Ring partitioning | Route/replicate via the consistent-hashing ring; never hardcode node addresses. |
| mTLS | All inter-node and client-node sockets must enforce mutual TLS. Never allow plaintext fallback. |
| Idempotent replay | Local log entries must be replayable without side-effects. |
| Hinted handoff ordering | Hints must carry the original ordering metadata of the deferred write. |

---

## Toolchain — environment and commands

The project uses [`uv`](https://docs.astral.sh/uv/) for all environment and
dependency management.

```bash
# Install / refresh dev environment
uv sync --extra dev

# Install git hooks (once per checkout)
uv run pre-commit install

# Run all quality checks
uv run pre-commit run --all-files

# Run tests with coverage
uv run pytest
```

All pre-commit hooks use `language: system` — they call tools installed in the
`.venv` managed by `uv`. Never suggest adding a hook that fetches an external
environment at hook runtime.

---

## Dependency rules

- Add runtime dependencies to `[project.dependencies]` in `pyproject.toml`.
- Add dev/tooling dependencies to `[project.optional-dependencies] dev`.
- Prefer stdlib; introduce a third-party dependency only when it meaningfully
  reduces complexity.
- Pin with `>=<min>` lower bounds only; avoid upper bounds unless a known
  incompatibility exists.

---

## Testing

- Test files live in `tests/` and mirror the `tourillon/` package structure.
- Every test file must carry the Apache 2.0 header.
- Use `pytest` with type-annotated `-> None` test functions.
- Test names: `test_<what>_<condition>_<expected>`.
- Cover both the happy path and explicit failure/edge cases.
- Do not use `unittest.TestCase`; use plain functions and `pytest.raises` /
  `pytest.mark`.

---

## Documentation

- `docs/` contains the normative specification. Do not contradict it in code
  comments or docstrings.
- Use RFC 2119 keywords (`MUST`, `SHOULD`, `MAY`) only in `docs/` markdown
  files, not in source code.
- Keep inline comments minimal; prefer self-documenting names and docstrings.

---

## What Copilot must never do

- Do not suggest `asyncio.get_event_loop()` (deprecated); use
  `asyncio.get_running_loop()` or `asyncio.run()`.
- Do not use `print()` for logging; use the stdlib `logging` module.
- Do not hard-code IP addresses, ports, or certificate paths.
- Do not bypass type annotations with `Any` unless truly unavoidable, and
  always add a `# noqa: ANN401` comment explaining why.
- Do not generate code that violates the architecture invariants above.
