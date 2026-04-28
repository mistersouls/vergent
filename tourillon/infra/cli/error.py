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
"""Error handling utilities for the Tourillon CLI.

Provides ExitError for expected user-facing failures and configure_error_hook
to install a sys.excepthook that renders friendly Rich panels instead of raw
Python tracebacks. The hook maps every exception category to an appropriate
exit code and display format:

- ExitError: rendered as a red panel using the embedded message.
- KeyboardInterrupt: silent "Interrupted." and exit 0.
- Any other exception: a generic "Unexpected error" panel with the exception
  type and message only; no traceback is ever shown to the operator.
"""

import sys
from dataclasses import dataclass, field
from types import TracebackType

import rich.console
import rich.panel
import rich.text


@dataclass
class ExitError(Exception):
    """Raised to signal a controlled exit with a readable message and exit code.

    Commands raise ExitError instead of calling sys.exit() directly so that the
    error hook can render the message consistently and tests can inspect the
    exception without process termination.
    """

    message: str
    exit_code: int = field(default=1)


def configure_error_hook(console: rich.console.Console) -> None:
    """Install a sys.excepthook that renders operator-friendly error messages.

    After this call, any unhandled exception that reaches the top-level event
    loop is intercepted and displayed as a Rich panel. Raw tracebacks are never
    shown. The function is idempotent: calling it multiple times replaces the
    previous hook.
    """

    def hook(
        exc_type: type[BaseException],
        exc: BaseException,
        _tb: TracebackType | None,
    ) -> None:
        if isinstance(exc, ExitError):
            console.print(
                rich.panel.Panel(
                    rich.text.Text(exc.message, style="bold red"),
                    title="[red]Error[/red]",
                    border_style="red",
                )
            )
            sys.exit(exc.exit_code)

        if issubclass(exc_type, KeyboardInterrupt):
            console.print("\nInterrupted.")
            sys.exit(0)

        console.print(
            rich.panel.Panel(
                rich.text.Text(f"{exc_type.__name__}: {exc}", style="bold red"),
                title="[red]Unexpected error[/red]",
                border_style="red",
            )
        )
        sys.exit(2)

    sys.excepthook = hook
