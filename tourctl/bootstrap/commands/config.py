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
"""Context management sub-commands for the tourctl CLI.

The config sub-application exposes two commands that navigate the
contexts file at ~/.config/tourillon/contexts.toml:

    tourctl config use-context NAME  — set the active context so that
        subsequent tourctl commands connect to the named cluster without
        requiring explicit connection flags.

    tourctl config list              — list all available contexts and
        mark the currently active one.

tourctl never issues certificates or writes PKI material. These commands
only read and navigate the contexts file written by
tourillon config generate-context.
"""

from pathlib import Path

import typer

from tourillon.bootstrap.contexts import load_contexts
from tourillon.core.config import ConfigError
from tourillon.infra.cli.output import print_error, print_info, print_success

app = typer.Typer(
    name="config",
    help="Manage tourctl connection contexts.",
    no_args_is_help=True,
)

_DEFAULT_CONTEXTS_PATH = Path.home() / ".config" / "tourillon" / "contexts.toml"


@app.command(name="use-context")
def use_context(
    name: str = typer.Argument(help="Context name to activate."),
    contexts_file: Path = typer.Option(
        _DEFAULT_CONTEXTS_PATH,
        "--contexts-file",
        hidden=True,
        help="Override the contexts file path.",
    ),
) -> None:
    """Set the active context for subsequent tourctl commands.

    After this command, tourctl kv get/put/delete and other operator commands
    will connect using the named context without requiring explicit --certfile,
    --keyfile, --cafile, or --host flags.
    """
    try:
        contexts = load_contexts(contexts_file)
    except ConfigError as exc:
        print_error(f"Failed to load contexts: {exc}")

    try:
        contexts.set_current(name)
    except ConfigError as exc:
        print_error(str(exc))

    try:
        contexts.save(contexts_file)
    except OSError as exc:
        print_error(f"Failed to save contexts: {exc}", exit_code=2)

    print_success(f"Active context set to {name!r}.")


@app.command(name="list")
def list_contexts(
    contexts_file: Path = typer.Option(
        _DEFAULT_CONTEXTS_PATH,
        "--contexts-file",
        hidden=True,
        help="Override the contexts file path.",
    ),
) -> None:
    """List all available contexts and mark the currently active one."""
    try:
        contexts = load_contexts(contexts_file)
    except ConfigError as exc:
        print_error(f"Failed to load contexts: {exc}")

    if not contexts.contexts:
        print_info(
            "No contexts found. Run tourillon config generate-context to create one."
        )
        return

    for ctx in contexts.contexts:
        marker = "* " if ctx.name == contexts.current_context else "  "
        kv = ctx.endpoints.kv or "—"
        peer = ctx.endpoints.peer or "—"
        print_info(f"{marker}{ctx.name}  (kv={kv}  peer={peer})")
