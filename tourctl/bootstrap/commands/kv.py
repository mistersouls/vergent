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
"""KV sub-commands for the tourctl CLI.

Exposes three commands — ``put``, ``get``, ``delete`` — that connect to a
running Tourillon node over mTLS and perform the corresponding operation using
the KV wire protocol defined in ``tourillon.bootstrap.handlers``.

Each command operates in one of two connection modes:

1. **Active context** (preferred): when no explicit --certfile / --keyfile /
   --cafile flags are supplied, the command reads the active context from
   ~/.config/tourillon/contexts.toml (set via ``tourctl config use-context``)
   and derives the host, port, and SSL context from the context's inline
   base64 TLS material.

2. **Explicit flags**: when --certfile, --keyfile, and --cafile are all
   supplied, the command uses those file paths directly and --host / --port
   must also be specified.

Connection options are optional in mode 1 (active context) and required in
mode 2 (explicit flags).
"""

import asyncio
from pathlib import Path

import typer

from tourctl.bootstrap import deps
from tourctl.core.client import NodeUnreachableError, RequestTimeoutError, ServerError
from tourillon.bootstrap.handlers import (
    KIND_KV_DELETE,
    KIND_KV_GET,
    KIND_KV_PUT,
)
from tourillon.core.config import ConfigError
from tourillon.core.net.tcp.tls import TlsConfigurationError
from tourillon.core.structure.envelope import Envelope
from tourillon.infra.cli.output import (
    print_error,
    print_key_value,
    print_success,
    print_warning,
)

app = typer.Typer(name="kv", help="Key-value operations against a Tourillon node.")

_CONNECTION_OPTIONS = "Connection"

_OPT_HOST = typer.Option(
    "127.0.0.1",
    "--host",
    rich_help_panel=_CONNECTION_OPTIONS,
    help="Node hostname or IP. Used when providing explicit cert flags.",
    show_default=True,
)
_OPT_PORT = typer.Option(
    7000,
    "--port",
    show_default=True,
    rich_help_panel=_CONNECTION_OPTIONS,
    help="Node TCP port. Used when providing explicit cert flags.",
)
_OPT_CERTFILE = typer.Option(
    None,
    "--certfile",
    rich_help_panel=_CONNECTION_OPTIONS,
    help="Client PEM certificate. Use with --keyfile and --cafile.",
)
_OPT_KEYFILE = typer.Option(
    None,
    "--keyfile",
    rich_help_panel=_CONNECTION_OPTIONS,
    help="Client PEM private key. Use with --certfile and --cafile.",
)
_OPT_CAFILE = typer.Option(
    None,
    "--cafile",
    rich_help_panel=_CONNECTION_OPTIONS,
    help="CA certificate bundle. Use with --certfile and --keyfile.",
)
_OPT_TIMEOUT = typer.Option(
    10.0,
    "--timeout",
    show_default=True,
    rich_help_panel=_CONNECTION_OPTIONS,
    help="Request deadline in seconds.",
)


def _setup_connection(
    host: str,
    port: int,
    certfile: Path | None,
    keyfile: Path | None,
    cafile: Path | None,
    timeout: float,
) -> None:
    """Configure deps for the active connection mode.

    When all three cert flags are supplied, use explicit file-path mode.
    Otherwise fall back to the active context from contexts.toml.
    Raise on any configuration error so the caller can surface a clean message.
    """
    explicit = certfile is not None and keyfile is not None and cafile is not None
    if explicit:
        deps.configure(host, port, certfile, keyfile, cafile, timeout=timeout)
    else:
        try:
            deps.configure_from_active_context(timeout=timeout)
        except ConfigError as exc:
            print_error(str(exc))


def _handle_client_error(
    exc: Exception,
    timeout: float,
) -> None:
    """Map known client errors to Rich output and exit."""
    match exc:
        case NodeUnreachableError():
            print_error("Cannot reach node — is it running?")
        case RequestTimeoutError():
            print_error(f"Request timed out after {timeout}s — node may be overloaded.")
        case ServerError():
            print_error(f"Node returned an error: {exc.message}")
        case ValueError():
            print_error(f"Unexpected response format: {exc}")
        case _:
            print_error(f"Unexpected error: {exc}")


@app.command()
def put(
    keyspace: str = typer.Option(
        "default",
        "--keyspace",
        "-n",
        show_default=True,
        help="Keyspace (logical namespace) for the key.",
    ),
    key: str = typer.Option(..., "--key", "-k", help="Key to write."),
    value: str = typer.Option(
        ..., "--value", "-v", help="Value to associate with the key."
    ),
    host: str = _OPT_HOST,
    port: int = _OPT_PORT,
    certfile: Path | None = _OPT_CERTFILE,
    keyfile: Path | None = _OPT_KEYFILE,
    cafile: Path | None = _OPT_CAFILE,
    timeout: float = _OPT_TIMEOUT,
) -> None:
    """Write a key/value pair into a keyspace on the target node."""
    try:
        _setup_connection(host, port, certfile, keyfile, cafile, timeout)
    except TlsConfigurationError as exc:
        print_error(str(exc))

    serializer = deps.get_serializer()

    async def _run() -> None:
        client = deps.get_client()
        payload = serializer.encode(
            {
                "keyspace": keyspace.encode(),
                "key": key.encode(),
                "value": value.encode(),
            }
        )
        response = await client.request(Envelope.create(payload, kind=KIND_KV_PUT))
        data = serializer.decode(response.payload)
        print_key_value(
            f"kv.put  {keyspace}/{key}",
            [
                ("wall", str(data.get("wall", "?"))),
                ("counter", str(data.get("counter", "?"))),
                ("node_id", str(data.get("node_id", "?"))),
            ],
        )

    try:
        asyncio.run(_run())
    except (NodeUnreachableError, RequestTimeoutError, ServerError, ValueError) as exc:
        _handle_client_error(exc, timeout)


@app.command()
def get(
    keyspace: str = typer.Option(
        "default", "--keyspace", "-n", show_default=True, help="Keyspace to read from."
    ),
    key: str = typer.Option(..., "--key", "-k", help="Key to read."),
    host: str = _OPT_HOST,
    port: int = _OPT_PORT,
    certfile: Path | None = _OPT_CERTFILE,
    keyfile: Path | None = _OPT_KEYFILE,
    cafile: Path | None = _OPT_CAFILE,
    timeout: float = _OPT_TIMEOUT,
) -> None:
    """Read the current value for a key from the target node."""
    try:
        _setup_connection(host, port, certfile, keyfile, cafile, timeout)
    except TlsConfigurationError as exc:
        print_error(str(exc))

    serializer = deps.get_serializer()

    async def _run() -> None:
        client = deps.get_client()
        payload = serializer.encode(
            {
                "keyspace": keyspace.encode(),
                "key": key.encode(),
            }
        )
        response = await client.request(Envelope.create(payload, kind=KIND_KV_GET))
        data = serializer.decode(response.payload)
        versions: list[dict] = data.get("versions", [])  # type: ignore[type-arg]

        if not versions:
            print_warning(f"Key {keyspace}/{key} not found (or deleted).")
            return

        v = versions[0]
        raw_value: bytes = v.get("value", b"")
        try:
            display_value = raw_value.decode()
        except UnicodeDecodeError:
            display_value = raw_value.hex()

        print_key_value(
            f"kv.get  {keyspace}/{key}",
            [
                ("value", display_value),
                ("wall", str(v.get("wall", "?"))),
                ("counter", str(v.get("counter", "?"))),
                ("node_id", str(v.get("node_id", "?"))),
            ],
        )

    try:
        asyncio.run(_run())
    except (NodeUnreachableError, RequestTimeoutError, ServerError, ValueError) as exc:
        _handle_client_error(exc, timeout)


@app.command()
def delete(
    keyspace: str = typer.Option(
        "default",
        "--keyspace",
        "-n",
        show_default=True,
        help="Keyspace containing the key.",
    ),
    key: str = typer.Option(..., "--key", "-k", help="Key to delete."),
    host: str = _OPT_HOST,
    port: int = _OPT_PORT,
    certfile: Path | None = _OPT_CERTFILE,
    keyfile: Path | None = _OPT_KEYFILE,
    cafile: Path | None = _OPT_CAFILE,
    timeout: float = _OPT_TIMEOUT,
) -> None:
    """Delete a key from the target node (produces a Tombstone)."""
    try:
        _setup_connection(host, port, certfile, keyfile, cafile, timeout)
    except TlsConfigurationError as exc:
        print_error(str(exc))

    serializer = deps.get_serializer()

    async def _run() -> None:
        client = deps.get_client()
        payload = serializer.encode(
            {
                "keyspace": keyspace.encode(),
                "key": key.encode(),
            }
        )
        response = await client.request(Envelope.create(payload, kind=KIND_KV_DELETE))
        data = serializer.decode(response.payload)
        print_success(
            f"Deleted {keyspace}/{key} — "
            f"wall={data.get('wall')} counter={data.get('counter')} "
            f"node={data.get('node_id')}"
        )

    try:
        asyncio.run(_run())
    except (NodeUnreachableError, RequestTimeoutError, ServerError, ValueError) as exc:
        _handle_client_error(exc, timeout)
