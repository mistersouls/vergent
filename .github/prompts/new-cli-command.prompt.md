---
mode: agent
description: Add a new tourctl or tourillon CLI sub-command backed by a TcpClient envelope exchange.
---

# Add a new CLI command

Command: **`${input:command}`**
(e.g. `tourctl node join`, `tourctl kv put`, `tourillon pki ca`)

Binary: **`${input:binary}`** (`tourctl` or `tourillon`)

## Step 1 — Read the proposal

Read the relevant proposal from `proposals/`. Find:
- The exact command signature (flags, arguments, defaults).
- The exact `stdout` messages for success and every error path
  (Copilot must match these strings **exactly** — operators write scripts
  against them).
- The envelope kind(s) this command sends / receives.
- The exit codes (`0` = success, `1` = error, `2` = partial / not found).

## Step 2 — File layout

```
${input:binary}/
  infra/
    cli/
      ${input:commandModule}.py   ← Typer app or sub-command callback
  core/
    commands/
      ${input:commandModule}.py   ← Domain function: builds Envelope, calls TcpClient
```

The CLI module wires Typer to the domain function.
The domain module knows nothing about Typer or `rich`.

## Step 3 — CLI module template

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""CLI entry point for `${input:command}`."""

from __future__ import annotations

import typer
from rich.console import Console

from ${input:binary}.core.commands.${input:commandModule} import run_${input:verb}

app = typer.Typer()
console = Console()
err_console = Console(stderr=True)


@app.command()
def ${input:verb}(
    # TODO: add flags/options matching the proposal's CLI contract exactly
) -> None:
    """<One-line description from the proposal summary>."""
    # Load context from contexts.toml (tourctl) or config.toml (tourillon).
    # Build TLS context.
    # Call run_${input:verb}(...) which issues the envelope exchange.
    # Print success/failure strings matching the proposal EXACTLY.
    # Call raise typer.Exit(code=N) for non-zero exits.
```

## Step 4 — Domain function template

```python
# Copyright 2026 Tourillon Contributors
# ... (full Apache 2.0 header)
"""Domain logic for the `${input:command}` operation."""

from __future__ import annotations

import logging

from tourillon.core.structure.envelope import Envelope

logger = logging.getLogger(__name__)


async def run_${input:verb}(
    client: "TcpClient",
    # TODO: typed domain parameters — no Typer types here
) -> ...:
    """Execute the `${input:command}` protocol exchange.

    <Describe: what envelopes are sent, what responses are expected,
    what return value is produced, and which exceptions it raises.>
    """
    env = Envelope(kind="<kind>", payload=...)
    response = await client.request(env)
    # TODO: parse response, return domain result
```

## Output string rules

- Success prefix: `✓ ` (U+2713 CHECK MARK + space)
- Failure prefix: `✗ ` (U+2717 BALLOT X + space)
- Progress prefix: `  ` (two spaces; no icon)
- Print to `Console(stderr=True)` for errors; stdout for data output.
- `tourctl kv get` writes **only the raw value** to stdout (so it can be piped).

## Exit code conventions

| Situation | Exit code |
|-----------|-----------|
| Success | `0` (default) |
| General error (bad flag, unreachable node, phase violation) | `1` |
| Partial / not found / quorum not met | `2` |

## Tests to write

Create appropriate test in `tests/` covering:
- Happy path — correct `stdout` / `stderr` strings.
- Every error path in the proposal's error table — correct message + exit code.
- Unknown / unexpected response envelope → sensible error, exit 1.
