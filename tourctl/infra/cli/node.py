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
"""tourctl node subcommands — operator-facing node inspection and join commands."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from tourctl.core.commands.config import ContextsError, load_contexts
from tourctl.core.commands.inspect import InspectCommand
from tourctl.core.commands.node_join import NodeJoinCommand
from tourillon.core.ports.transport import RESPONSE_TIMEOUT
from tourillon.core.transport.client import TcpClient
from tourillon.infra.serializer.msgpack import MsgpackSerializerAdapter
from tourillon.infra.tls.context import build_client_ssl_context

node_app = typer.Typer(no_args_is_help=True)

_console = Console()
_err_console = Console(stderr=True)

_DEFAULT_CONTEXTS_PATH = Path.home() / ".config" / "tourillon" / "contexts.toml"


@node_app.command("inspect")
def node_inspect(
    peer_address: Annotated[
        str,
        typer.Argument(
            help="Peer server address of the node to inspect (e.g. 127.0.0.1:7701)"
        ),
    ],
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Response timeout in seconds"),
    ] = RESPONSE_TIMEOUT,
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Emit raw JSON to stdout"),
    ] = False,
    show_all_partitions: Annotated[
        bool,
        typer.Option("--partitions", help="List every partition range individually"),
    ] = False,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
) -> None:
    """Inspect a node's live state by connecting directly to PEER_ADDRESS.

    Per proposal 003 rev 2 the operator supplies the target node's peer
    address directly; tourctl opens a direct mTLS connection and the node
    builds the response from its own in-memory state. No forwarding occurs.
    TLS credentials are taken from the active context.
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
        _run_inspect(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            peer_address,
            show_all_partitions=show_all_partitions,
            json_output=json_output,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


@node_app.command("join")
def node_join(
    peer_address: Annotated[
        str,
        typer.Argument(
            help="Peer server address of the target node (e.g. 10.0.0.2:7701)"
        ),
    ],
    seeds: Annotated[
        list[str] | None,
        typer.Option("--seeds", help="Seed addresses (overrides config.toml seeds)"),
    ] = None,
    timeout: Annotated[
        float,
        typer.Option("--timeout", help="Response timeout in seconds"),
    ] = RESPONSE_TIMEOUT,
    contexts_path: Annotated[Path, typer.Option("--contexts")] = _DEFAULT_CONTEXTS_PATH,
) -> None:
    """Trigger the IDLE → JOINING transition on a running daemon.

    Connects directly to PEER_ADDRESS (the target node's peer server).
    TLS credentials are taken from the active context.
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
        _run_join(
            ctx.credentials.cert_data,
            ctx.credentials.key_data,
            ctx.cluster.ca_data,
            peer_address,
            seeds=seeds,
            timeout=timeout,
        )
    )
    raise typer.Exit(exit_code)


async def _run_inspect(
    cert_data: str,
    key_data: str,
    ca_data: str,
    peer_endpoint: str,
    *,
    show_all_partitions: bool,
    json_output: bool,
    timeout: float,
) -> int:
    """Connect to *peer_endpoint* and run the inspect command (self-inspect only)."""
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(peer_endpoint, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ {peer_endpoint} is unreachable ({exc}).")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = InspectCommand(
        client=client,
        serializer=serializer,
        console=_console,
        err_console=_err_console,
        target_address=peer_endpoint,
    )
    try:
        return await cmd.run(
            show_all_partitions=show_all_partitions,
            json_output=json_output,
            timeout=timeout,
        )
    finally:
        await client.close()


async def _run_join(
    cert_data: str,
    key_data: str,
    ca_data: str,
    peer_address: str,
    *,
    seeds: list[str] | None,
    timeout: float,
) -> int:
    """Connect directly to the target peer address and send node.join."""
    tls_ctx = build_client_ssl_context(cert_data, key_data, ca_data)
    client = TcpClient()
    try:
        await client.connect(peer_address, tls_ctx)
    except OSError as exc:
        _err_console.print(f"✗ Cannot connect to {peer_address}: {exc}")
        return 1

    serializer = MsgpackSerializerAdapter()
    cmd = NodeJoinCommand(
        client=client,
        serializer=serializer,
        console=_console,
        err_console=_err_console,
        peer_address=peer_address,
    )
    try:
        return await cmd.run(seeds=seeds, timeout=timeout)
    finally:
        await client.close()
