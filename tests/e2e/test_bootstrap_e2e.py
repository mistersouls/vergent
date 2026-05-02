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
"""E2E tests — scenarios 13 and 14 (require real mTLS sockets)."""

from __future__ import annotations

import asyncio
import contextlib
import ssl
import stat
from pathlib import Path

import pytest

from tourillon.core.transport.dispatcher import Dispatcher
from tourillon.core.transport.server import TcpServer
from tourillon.infra.tls.context import (
    build_client_ssl_context,
    build_server_ssl_context,
)


@pytest.mark.e2e
@pytest.mark.bootstrap
async def test_13_mtls_connection_without_client_cert_fails_handshake(
    ca_material: tuple[bytes, bytes],
    leaf_material: tuple[bytes, bytes],
) -> None:
    """Connection is rejected when the client certificate is signed by an untrusted CA.

    Under TLS 1.2 the rejection happens at open_connection time; under TLS 1.3
    it may happen on the first I/O after the handshake. Both are acceptable
    outcomes: the point is that no application-level response is delivered.
    """
    import base64

    from tests.conftest import _generate_ca, _generate_leaf

    ca_cert_pem, _ = ca_material
    leaf_cert_pem, leaf_key_pem = leaf_material
    ca_b64 = base64.b64encode(ca_cert_pem).decode()
    cert_b64 = base64.b64encode(leaf_cert_pem).decode()
    key_b64 = base64.b64encode(leaf_key_pem).decode()

    server_ctx = build_server_ssl_context(cert_b64, key_b64, ca_b64)
    dispatcher = Dispatcher()
    srv = await asyncio.start_server(
        TcpServer(dispatcher, ssl_context=server_ctx)._handle_connection,
        "127.0.0.1",
        0,
        ssl=server_ctx,
    )
    addr = srv.sockets[0].getsockname()

    # Client presents a cert from a foreign CA — server must not accept it.
    other_ca_cert_pem, other_ca_key_pem = _generate_ca()
    other_leaf_cert_pem, other_leaf_key_pem = _generate_leaf(
        other_ca_cert_pem, other_ca_key_pem
    )
    untrusted_ctx = build_client_ssl_context(
        base64.b64encode(other_leaf_cert_pem).decode(),
        base64.b64encode(other_leaf_key_pem).decode(),
        ca_b64,  # client trusts the correct server CA for server-cert verification
    )

    rejected = False
    writer: asyncio.StreamWriter | None = None
    try:
        reader, writer = await asyncio.open_connection(
            addr[0], addr[1], ssl=untrusted_ctx
        )
        # open_connection may succeed on TLS 1.3; probe with an I/O to force rejection.
        writer.write(b"\x00" * 24)
        try:
            await asyncio.wait_for(writer.drain(), timeout=3.0)
            data = await asyncio.wait_for(reader.read(1024), timeout=3.0)
            # An empty read means the server closed the connection.
            rejected = not data
        except (TimeoutError, ssl.SSLError, OSError, ConnectionResetError):
            rejected = True
    except (ssl.SSLError, OSError, ConnectionResetError):
        rejected = True
    finally:
        if writer is not None:
            writer.close()
        srv.close()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(srv.wait_closed(), timeout=2.0)

    assert rejected, "Expected the untrusted client certificate to be rejected"


@pytest.mark.e2e
@pytest.mark.bootstrap
async def test_14_full_provisioning_session_files_at_0600(tmp_path: Path) -> None:
    """pki ca → config generate → config generate-context → tourctl config use-context succeed; files at 0600."""
    import os
    import sys

    from typer.testing import CliRunner

    from tourctl.bootstrap.main import app as tourctl_app
    from tourillon.infra.cli.main import app as tourillon_app

    runner = CliRunner()
    ca_cert = tmp_path / "ca.crt"
    ca_key_path = tmp_path / "ca.key"
    node_cfg = tmp_path / "node-1.toml"
    contexts_file = tmp_path / "contexts.toml"

    # Step 1: tourillon pki ca
    result = runner.invoke(
        tourillon_app,
        ["pki", "ca", "--out-cert", str(ca_cert), "--out-key", str(ca_key_path)],
    )
    assert result.exit_code == 0, result.output
    assert ca_cert.exists()
    assert ca_key_path.exists()
    if sys.platform != "win32":
        assert stat.S_IMODE(os.stat(ca_key_path).st_mode) == 0o600

    # Step 2: tourillon config generate
    result = runner.invoke(
        tourillon_app,
        [
            "config",
            "generate",
            "--ca-cert",
            str(ca_cert),
            "--ca-key",
            str(ca_key_path),
            "--node-id",
            "node-1",
            "--out",
            str(node_cfg),
        ],
    )
    assert result.exit_code == 0, result.output
    assert node_cfg.exists()
    if sys.platform != "win32":
        assert stat.S_IMODE(os.stat(node_cfg).st_mode) == 0o600

    # Step 3: tourillon config generate-context
    result = runner.invoke(
        tourillon_app,
        [
            "config",
            "generate-context",
            "prod",
            "--ca-cert",
            str(ca_cert),
            "--ca-key",
            str(ca_key_path),
            "--kv",
            "kv.prod.example.com:7700",
            "--peer",
            "peer.prod.example.com:7701",
            "--out",
            str(contexts_file),
        ],
    )
    assert result.exit_code == 0, result.output
    assert contexts_file.exists()
    if sys.platform != "win32":
        assert stat.S_IMODE(os.stat(contexts_file).st_mode) == 0o600

    # Step 4: tourctl config use-context
    result = runner.invoke(
        tourctl_app,
        ["config", "use-context", "prod", "--contexts", str(contexts_file)],
    )
    assert result.exit_code == 0, result.output
