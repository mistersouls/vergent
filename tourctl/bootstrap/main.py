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
"""Root Typer application for the tourctl CLI.

Command tree:

    tourctl
        version          — print the installed tourctl version
        kv
            put          — write a key/value pair
            get          — read a key
            delete       — delete a key
"""

import importlib.metadata

import typer

from tourctl.bootstrap.commands import kv
from tourillon.infra.cli.error import configure_error_hook
from tourillon.infra.cli.output import make_console, print_info

app = typer.Typer(
    name="tourctl",
    help="tourctl — Tourillon KV client.",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
)

app.add_typer(kv.app)

configure_error_hook(make_console())


@app.command()
def version() -> None:
    """Print the installed tourctl version."""
    try:
        ver = importlib.metadata.version("tourctl")
    except importlib.metadata.PackageNotFoundError:
        ver = "dev"
    print_info(f"tourctl {ver}")
