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
"""tourctl rebalance subcommands — rebalance status inspection."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from tourctl.core.commands.config import ContextsError, load_contexts
from tourctl.core.commands.rebalance import RebalanceStatusCommand
from tourillon.core.ports.transport import RESPONSE_TIMEOUT
from tourillon.core.transport.client import TcpClient
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import build_client_ssl_context

rebalance_app = typer.Typer(no_args_is_help=True)

_console = Console()
_err_console = Console(stderr=True)

_DEFAULT_CONTEXTS_PATH = Path.home() / ".config" / "tourillon" / "contexts.toml"


@rebalance_app.command("status")
def rebalance_status(
    peer_address: Annotated[
        str,
        typer.Argument(
            help="Peer server address of the node to query (e.g. 127.0.0.1:7701)"
        ),
    ],
    after_pid: Annotated[
        int,
        typer.Option(
            "--after-pid", help="Paginate from this pid (exclusive lower bound)"
        ),
    ] = 0,
    limit: Annotated[
        int,
        typer.Option("--limit", help="Max transfer entries per page"),
    ] = 500,
    blocked: Annotated[
        bool,
        typer.Option("--blocked", help="Show only FAILED transfers and operator hint"),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit raw JSON to stdout"),
    ] = False,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Response timeout in seconds"),
    ] = RESPONSE_TIMEOUT,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
) -> None:
    """Show the rebalance status of the node at PEER_ADDRESS.

    Exit code 0 on success, 1 on any error, 2 when --blocked and the node
    is BLOCKED (one or more FAILED transfers).
    """
    try:
        contexts_file = load_contexts(contexts_path)
    except ContextsError as exc:
        _err_console.print(f"✗ {exc}")
        raise typer.Exit(1) from exc

    ctx = (
        contexts_file.get(contexts_file.current_context)
        if contexts_file.current_context
        else None
    )
    if ctx is None:
        _err_console.print("✗ No active context. Use `tourctl config use-context`.")
        raise typer.Exit(1)

    exit_code = asyncio.run(
        _run_status(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            peer_address,
            after_pid=after_pid,
            limit=limit,
            blocked_only=blocked,
            json_output=json_output,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


async def _run_status(
    cert_data: str,
    key_data: str,
    ca_data: str,
    peer_endpoint: str,
    *,
    after_pid: int,
    limit: int,
    blocked_only: bool,
    json_output: bool,
    timeout: float,
) -> int:
    """Connect to the peer endpoint and run the rebalance status command."""
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(peer_endpoint, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ Cannot connect to {peer_endpoint}: {exc}")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = RebalanceStatusCommand(
        client=client,
        serializer=serializer,
        console=_console,
        err_console=_err_console,
    )
    try:
        return await cmd.run(
            after_pid=after_pid,
            limit=limit,
            blocked_only=blocked_only,
            json_output=json_output,
            timeout=timeout,
        )
    finally:
        await client.close()
