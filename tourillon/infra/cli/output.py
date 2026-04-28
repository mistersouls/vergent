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
"""Rich-based output helpers for the Tourillon CLI.

All user-facing output goes through the helpers in this module so that
formatting is consistent across commands. Never use print() directly.
"""

import sys

import rich.console
import rich.panel
import rich.table
import rich.text


def make_console() -> rich.console.Console:
    """Create and return a Console instance with syntax highlighting disabled.

    Syntax highlighting is off by default because Tourillon output consists of
    paths, identifiers, and status messages — not source code — and automatic
    highlighting would produce misleading colours.
    """
    return rich.console.Console(highlight=False)


_console: rich.console.Console = make_console()


def print_success(message: str) -> None:
    """Print a green success panel."""
    _console.print(
        rich.panel.Panel(
            rich.text.Text(f"✓  {message}", style="bold green"),
            border_style="green",
        )
    )


def print_error(message: str, exit_code: int = 1) -> None:
    """Print a red error panel and exit the process with exit_code."""
    _console.print(
        rich.panel.Panel(
            rich.text.Text(message, style="bold red"),
            title="[red]Error[/red]",
            border_style="red",
        )
    )
    sys.exit(exit_code)


def print_info(message: str) -> None:
    """Print an informational line in cyan."""
    _console.print(rich.text.Text(f"ℹ  {message}", style="cyan"))


def print_warning(message: str) -> None:
    """Print a warning line in yellow."""
    _console.print(rich.text.Text(f"⚠  {message}", style="yellow"))


def print_key_value(title: str, rows: list[tuple[str, str]]) -> None:
    """Print a titled panel containing a two-column key/value table."""
    table = rich.table.Table(show_header=False, box=None, padding=(0, 1))
    table.add_column(justify="right", style="bold")
    table.add_column()
    for key, value in rows:
        table.add_row(key, value)
    _console.print(rich.panel.Panel(table, title=title))
