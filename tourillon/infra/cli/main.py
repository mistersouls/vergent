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
"""Root Typer application for the Tourillon CLI.

This module assembles the command tree and installs the error hook that
prevents raw Python tracebacks from reaching the operator. It is the sole
entry point referenced by [project.scripts] in pyproject.toml.

Command tree:

    tourillon
        version          — print the installed package version
        pki
            ca           — generate a self-signed CA
            server       — issue a server certificate
            client       — issue a client certificate
        node
            start        — start a node with mTLS

Shell autocompletion (bash, zsh, fish, PowerShell) is available via:

    tourillon --install-completion
"""

import importlib.metadata

import typer

from tourillon.infra.cli.commands import node, pki
from tourillon.infra.cli.error import configure_error_hook
from tourillon.infra.cli.output import make_console, print_info

app = typer.Typer(
    name="tourillon",
    help="Tourillon — leaderless distributed key-value database.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

app.add_typer(pki.app)
app.add_typer(node.app)

configure_error_hook(make_console())


@app.command()
def version() -> None:
    """Print the installed Tourillon version."""
    ver = importlib.metadata.version("tourillon")
    print_info(f"tourillon {ver}")


if __name__ == "__main__":  # pragma: no cover
    app()
