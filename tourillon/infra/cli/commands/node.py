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
"""CLI sub-commands for Tourillon node lifecycle management.

The node sub-application currently exposes a single command, start, which
assembles a fully wired TcpServer with mTLS and hands it to NodeRunner for
supervised execution. Future commands (stop, status, join, leave) will be
added to this sub-application as the ring layer matures.

The start command validates all certificate paths before entering the asyncio
event loop so that configuration errors surface immediately with a clear
message rather than at the first incoming connection.
"""

import asyncio
import logging
from pathlib import Path

import typer

from tourillon.bootstrap.node import create_tcp_node
from tourillon.core.net.tcp.tls import TlsConfigurationError
from tourillon.infra.cli.output import print_error, print_info, print_success
from tourillon.infra.cli.runner import NodeRunner

app = typer.Typer(
    name="node",
    help="Manage Tourillon node lifecycle.",
    no_args_is_help=True,
)


@app.command()
def start(
    node_id: str = typer.Option(
        ..., "--node-id", help="Logical identifier for this node within the ring."
    ),
    host: str = typer.Option(
        "0.0.0.0",
        "--host",
        help="Bind address for the TCP listener.",
        show_default=True,
    ),
    port: int = typer.Option(
        7000, "--port", help="Bind port for the TCP listener.", show_default=True
    ),
    certfile: Path = typer.Option(
        ..., "--certfile", help="Path to the node PEM certificate file."
    ),
    keyfile: Path = typer.Option(
        ..., "--keyfile", help="Path to the node PEM private key file."
    ),
    cafile: Path = typer.Option(
        ..., "--cafile", help="Path to the CA certificate bundle for peer verification."
    ),
) -> None:
    """Start a Tourillon node with mTLS and listen for connections.

    All three certificate paths are validated before the asyncio event loop
    starts. The process blocks until SIGINT or SIGTERM is received, then
    performs a clean shutdown. Exit code is 0 on clean shutdown, 1 on
    configuration error, and 2 on unexpected runtime failure.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    for label, path in [
        ("certfile", certfile),
        ("keyfile", keyfile),
        ("cafile", cafile),
    ]:
        if not path.exists():
            print_error(f"{label} not found: {path}")

    try:
        server = asyncio.run(
            create_tcp_node(
                node_id,
                host,
                port,
                certfile,
                keyfile,
                cafile,
            )
        )
    except TlsConfigurationError as exc:
        print_error(f"TLS configuration error: {exc}")
        return
    except Exception as exc:
        print_error(f"Failed to assemble node: {exc}", exit_code=2)
        return

    print_info(f"Node {node_id!r} starting on {host}:{port}")
    print_info("Press Ctrl-C or send SIGTERM to stop.")
    NodeRunner(server).run()
    print_success(f"Node {node_id!r} stopped.")
