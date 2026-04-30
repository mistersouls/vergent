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

The node sub-application exposes a single command, start, which loads
TourillonConfig from a TOML file, wires a TcpServer with mTLS from inline
base64 certificate data, and hands the server to NodeRunner for supervised
execution.

Callers pass the config file path via --config and may append individual
override flags (--node-id, --log-level) to shadow the corresponding file
values without editing the file. All certificate material is read from the
config's inline base64 fields; no separate --certfile / --keyfile / --cafile
flags are required or accepted.

Future commands (stop, status, join, leave) will be added to this
sub-application as the ring layer matures.
"""

import logging
from pathlib import Path

import typer

from tourillon.bootstrap.config import load_config
from tourillon.bootstrap.node import create_tcp_node_from_config
from tourillon.core.config import ConfigError
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
    config_path: Path = typer.Option(
        ...,
        "--config",
        help="Path to the node TOML config file.",
    ),
    node_id: str = typer.Option(
        "",
        "--node-id",
        help="Override node.id from the config file.",
    ),
    log_level: str = typer.Option(
        "",
        "--log-level",
        help="Override node.log_level from the config file.",
    ),
) -> None:
    """Start a Tourillon node using a TOML config file.

    Load TourillonConfig from --config, validate all TLS material and config
    values, then bind the KV TCP listener with mTLS. The node blocks until
    SIGINT or SIGTERM is received, then performs a clean shutdown. Exit code
    is 0 on clean shutdown, 1 on configuration error, and 2 on unexpected
    runtime failure.

    Individual override flags (--node-id, --log-level) shadow the
    corresponding config file values when supplied.
    """
    try:
        cfg = load_config(
            config_path,
            node_id=node_id or None,
            log_level=log_level or None,
        )
    except ConfigError as exc:
        print_error(f"Configuration error: {exc}")

    logging.basicConfig(
        level=getattr(logging, cfg.log_level.upper(), logging.INFO),
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        import asyncio

        server = asyncio.run(create_tcp_node_from_config(cfg))
    except TlsConfigurationError as exc:
        print_error(f"TLS configuration error: {exc}")
    except Exception as exc:
        print_error(f"Failed to assemble node: {exc}", exit_code=2)

    bind = cfg.servers_kv.bind
    print_info(f"Node {cfg.node_id!r} starting on {bind} (KV listener)")
    print_info("Press Ctrl-C or send SIGTERM to stop.")
    NodeRunner(server).run()
    print_success(f"Node {cfg.node_id!r} stopped.")
