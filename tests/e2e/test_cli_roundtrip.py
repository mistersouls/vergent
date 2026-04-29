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
"""End-to-end CLI round-trip tests: tourillon node start + tourctl kv commands.

Each test in this module launches a real ``tourillon node start`` subprocess,
waits for the TCP listener to become reachable, then drives ``tourctl kv put``,
``tourctl kv get``, and ``tourctl kv delete`` as child processes and asserts on
their exit codes and captured stdout.

This layer complements ``test_kv_roundtrip.py`` (which exercises the protocol
stack in-process) by validating the full user-facing surface: argument parsing,
TLS context assembly from CLI flags, Rich output rendering, and process exit
codes. A regression in any of those layers would be caught here but not by the
protocol-level tests.

The fixture is function-scoped. Each test gets an isolated node bound to an
ephemeral port with a freshly generated certificate bundle, so tests are
independent and repeatable.
"""

import asyncio
import socket
import sys
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import NamedTuple

import pytest

from tourillon.core.ports.pki import CaRequest, CertRequest
from tourillon.infra.pki.x509 import X509CertificateAuthority


class _CliNode(NamedTuple):
    """Connection parameters for a running CLI test node.

    The subprocess itself is managed exclusively by the fixture. Tests interact
    only through ``_run_tourctl``, which builds the correct argument list from
    this structure.
    """

    host: str
    port: int
    ca_cert: Path
    cli_cert: Path
    cli_key: Path


def _free_port() -> int:
    """Return an OS-assigned free TCP port on 127.0.0.1.

    Binds a socket to port 0, reads the assigned port, and closes the socket
    immediately. There is a brief window between close and the server binding
    in which another process could claim the port, but in practice this is
    negligible in a test environment.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_for_port(host: str, port: int, *, timeout: float) -> None:
    """Poll until a TCP listener is accepting connections on host:port.

    Opens a plain TCP connection (no TLS) every 100 ms until the 3-way
    handshake succeeds. This is sufficient as a readiness probe because the
    server socket accepts connections at the TCP layer before the TLS handshake
    begins. Raises TimeoutError via asyncio.timeout if the port does not
    become reachable within the given deadline.
    """
    async with asyncio.timeout(timeout):
        while True:
            try:
                _, writer = await asyncio.open_connection(host, port)
                writer.close()
                await writer.wait_closed()
                return
            except OSError:
                await asyncio.sleep(0.1)


async def _run_tourctl(node: _CliNode, *args: str) -> tuple[int, str]:
    """Run a tourctl subcommand against node and return (returncode, stdout).

    Appends the four connection flags (--host, --port, --certfile, --keyfile,
    --cafile) to args so that callers only need to supply the subcommand and
    its own options. stderr is captured and discarded; the decoded stdout is
    returned for assertion.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-X",
        "utf8",
        "-m",
        "tourctl",
        *args,
        "--host",
        node.host,
        "--port",
        str(node.port),
        "--certfile",
        str(node.cli_cert),
        "--keyfile",
        str(node.cli_key),
        "--cafile",
        str(node.ca_cert),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, _ = await proc.communicate()
    assert proc.returncode is not None
    return proc.returncode, stdout.decode(errors="replace")


@pytest.fixture
async def cli_node(tmp_path: Path) -> AsyncGenerator[_CliNode]:
    """Start a tourillon node subprocess and yield its connection parameters.

    The fixture generates a self-signed CA, a server certificate (SAN IP
    127.0.0.1), and a client certificate. It then finds a free port, launches
    ``python -m tourillon node start`` as an asyncio subprocess, and polls the
    port until the TCP listener is ready. The server process is terminated in
    the finally block regardless of whether the test passes or fails.

    Certificate generation is offloaded to a thread via asyncio.to_thread
    because the X509CertificateAuthority methods are synchronous and CPU-bound.
    """
    pki = X509CertificateAuthority()

    ca_cert = tmp_path / "ca.pem"
    ca_key = tmp_path / "ca-key.pem"
    await asyncio.to_thread(
        pki.generate_ca,
        CaRequest(
            common_name="Tourillon CLI E2E CA",
            valid_days=1,
            key_size=2048,
            out_cert=ca_cert,
            out_key=ca_key,
        ),
    )

    srv_cert = tmp_path / "srv.pem"
    srv_key = tmp_path / "srv-key.pem"
    await asyncio.to_thread(
        pki.issue_cert,
        CertRequest(
            common_name="127.0.0.1",
            san_dns=(),
            san_ip=("127.0.0.1",),
            valid_days=1,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=srv_cert,
            out_key=srv_key,
            key_size=2048,
        ),
    )

    cli_cert = tmp_path / "cli.pem"
    cli_key = tmp_path / "cli-key.pem"
    await asyncio.to_thread(
        pki.issue_cert,
        CertRequest(
            common_name="cli-e2e-client",
            san_dns=(),
            san_ip=(),
            valid_days=1,
            ca_cert=ca_cert,
            ca_key=ca_key,
            out_cert=cli_cert,
            out_key=cli_key,
            key_size=2048,
        ),
    )

    port = _free_port()
    srv_proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-X",
        "utf8",
        "-m",
        "tourillon",
        "node",
        "start",
        "--node-id",
        "cli-e2e",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--certfile",
        str(srv_cert),
        "--keyfile",
        str(srv_key),
        "--cafile",
        str(ca_cert),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        await _wait_for_port("127.0.0.1", port, timeout=15.0)
        yield _CliNode(
            host="127.0.0.1",
            port=port,
            ca_cert=ca_cert,
            cli_cert=cli_cert,
            cli_key=cli_key,
        )
    finally:
        srv_proc.terminate()
        await srv_proc.wait()


async def test_cli_kv_put_exits_zero_and_reports_hlc(cli_node: _CliNode) -> None:
    """tourctl kv put must exit 0 and print the HLC metadata of the write."""
    code, out = await _run_tourctl(
        cli_node,
        "kv",
        "put",
        "--keyspace",
        "ns",
        "--key",
        "greeting",
        "--value",
        "hello",
    )
    assert code == 0
    assert "wall" in out


async def test_cli_kv_get_returns_stored_value(cli_node: _CliNode) -> None:
    """tourctl kv get must display the value previously written with kv put."""
    await _run_tourctl(
        cli_node, "kv", "put", "--keyspace", "ns", "--key", "city", "--value", "Paris"
    )
    code, out = await _run_tourctl(
        cli_node, "kv", "get", "--keyspace", "ns", "--key", "city"
    )
    assert code == 0
    assert "Paris" in out


async def test_cli_kv_get_missing_key_exits_zero_and_warns(cli_node: _CliNode) -> None:
    """tourctl kv get on an absent key must exit 0 and warn the operator."""
    code, out = await _run_tourctl(
        cli_node, "kv", "get", "--keyspace", "ns", "--key", "no-such-key"
    )
    assert code == 0
    assert "not found" in out


async def test_cli_kv_delete_then_get_warns_not_found(cli_node: _CliNode) -> None:
    """After kv delete the key must no longer be visible via kv get."""
    await _run_tourctl(
        cli_node, "kv", "put", "--keyspace", "ns", "--key", "temp", "--value", "gone"
    )
    code_del, out_del = await _run_tourctl(
        cli_node, "kv", "delete", "--keyspace", "ns", "--key", "temp"
    )
    assert code_del == 0
    assert "Deleted" in out_del

    code_get, out_get = await _run_tourctl(
        cli_node, "kv", "get", "--keyspace", "ns", "--key", "temp"
    )
    assert code_get == 0
    assert "not found" in out_get
