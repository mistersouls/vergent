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
"""Tests for RebalanceStatusCommand — proposal 005 tourctl CLI scenarios."""

from __future__ import annotations

import json
from typing import Any

import pytest

from tourctl.core.commands.rebalance import RebalanceStatusCommand
from tourillon.core.ports.transport import ResponseTimeoutError
from tourillon.core.structure.envelope import Envelope


class _MemConsole:
    """In-memory console that records printed lines."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def print(self, text: str = "") -> None:
        self.lines.append(text)


class _MockSerializer:
    schema_id = 1

    def encode(self, obj: Any) -> bytes:
        return json.dumps(obj, default=str).encode()

    def decode(self, data: bytes) -> Any:
        return json.loads(data)


class _FakeClient:
    def __init__(self, response: Envelope) -> None:
        self._response = response

    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        return self._response


class _TimeoutClient:
    async def request(self, env: Envelope, timeout: float = 30.0) -> Envelope:
        raise ResponseTimeoutError("timed out")


def _status_response(
    *,
    epoch: int | None = None,
    trigger: str = "joining",
    role: str = "receiving",
    blocked: bool = False,
    active: int = 0,
    summary: dict[str, int] | None = None,
    transfers: list[dict[str, Any]] | None = None,
    inactive: int = 0,
) -> dict[str, Any]:
    """Build a minimal rebalance.status.response payload dict."""
    return {
        "epoch": epoch,
        "trigger": trigger,
        "role": role,
        "blocked": blocked,
        "active_partitions": active,
        "inactive_partitions": inactive,
        "summary": summary
        or {"committed": 0, "running": 0, "pending": 0, "failed": 0, "cancelled": 0},
        "has_more": False,
        "next_pid": None,
        "transfers": transfers or [],
    }


def _cmd(client: Any = None) -> tuple[RebalanceStatusCommand, _MemConsole, _MemConsole]:
    ser = _MockSerializer()
    console = _MemConsole()
    err = _MemConsole()
    if client is None:
        client = _FakeClient(
            Envelope(
                kind="rebalance.status.response", payload=ser.encode(_status_response())
            )
        )
    return (
        RebalanceStatusCommand(
            client=client, serializer=ser, console=console, err_console=err
        ),
        console,
        err,
    )


@pytest.mark.rebalance
async def test_24_status_command_active_plan_renders_table() -> None:
    """Active JOINING plan → header, summary and transfer table are rendered; exit code 0."""
    ser = _MockSerializer()
    transfers = [
        {
            "pid": 42,
            "src": "node-2",
            "dst": "node-3",
            "state": "committed",
            "chunks_done": 128,
            "chunks_total": 128,
            "bytes_done": 4_404_019,
            "started_at": "2026-05-10T14:23:33Z",
            "finished_at": "2026-05-10T14:23:45Z",
            "last_error": None,
        },
    ]
    payload = _status_response(
        epoch=4,
        trigger="joining",
        role="receiving",
        blocked=False,
        active=1,
        summary={
            "committed": 1,
            "running": 0,
            "pending": 0,
            "failed": 0,
            "cancelled": 0,
        },
        transfers=transfers,
    )
    client = _FakeClient(
        Envelope(kind="rebalance.status.response", payload=ser.encode(payload))
    )
    cmd, console, err = _cmd(client)

    code = await cmd.run()

    assert code == 0
    output = "\n".join(console.lines)
    assert "epoch 4" in output
    assert "COMMITTED" in output
    assert err.lines == []


@pytest.mark.rebalance
async def test_25_status_command_no_plan_prints_message() -> None:
    """No active rebalance → 'No active rebalance' message; exit code 0."""
    ser = _MockSerializer()
    payload = _status_response(epoch=None)
    client = _FakeClient(
        Envelope(kind="rebalance.status.response", payload=ser.encode(payload))
    )
    cmd, console, err = _cmd(client)

    code = await cmd.run()

    assert code == 0
    assert any("No active rebalance" in line for line in console.lines)


@pytest.mark.rebalance
async def test_status_command_timeout_returns_exit_1() -> None:
    """ResponseTimeoutError → error message on stderr; exit code 1."""
    cmd, console, err = _cmd(_TimeoutClient())

    code = await cmd.run(timeout=5.0)

    assert code == 1
    assert any("timed out" in line.lower() for line in err.lines)


@pytest.mark.rebalance
async def test_status_command_error_kind_returns_exit_1() -> None:
    """Error kind response → error message; exit code 1."""
    client = _FakeClient(Envelope(kind="error.proto_version_unsupported", payload=b""))
    cmd, console, err = _cmd(client)

    code = await cmd.run()

    assert code == 1


@pytest.mark.rebalance
async def test_status_command_blocked_renders_hint_exit_2() -> None:
    """BLOCKED node + --blocked flag → operator hint + exit code 2."""
    ser = _MockSerializer()
    transfers = [
        {
            "pid": 44,
            "src": "node-2",
            "dst": "node-3",
            "state": "failed",
            "chunks_done": 0,
            "chunks_total": None,
            "bytes_done": 0,
            "started_at": None,
            "finished_at": None,
            "last_error": "source unreachable after 10 retries",
        },
    ]
    payload = _status_response(
        epoch=4,
        blocked=True,
        active=1,
        summary={
            "committed": 0,
            "running": 0,
            "pending": 0,
            "failed": 1,
            "cancelled": 0,
        },
        transfers=transfers,
    )
    client = _FakeClient(
        Envelope(kind="rebalance.status.response", payload=ser.encode(payload))
    )
    cmd, console, err = _cmd(client)

    code = await cmd.run(blocked_only=True)

    assert code == 2
    output = "\n".join(console.lines)
    assert "FAILED" in output
    assert "Operator hint" in output


@pytest.mark.rebalance
async def test_status_command_json_output() -> None:
    """--json flag → raw JSON printed; exit code 0."""
    ser = _MockSerializer()
    payload = _status_response(epoch=3, active=2)
    client = _FakeClient(
        Envelope(kind="rebalance.status.response", payload=ser.encode(payload))
    )
    cmd, console, err = _cmd(client)

    code = await cmd.run(json_output=True)

    assert code == 0
    assert len(console.lines) == 1
    parsed = json.loads(console.lines[0])
    assert parsed["epoch"] == 3


@pytest.mark.rebalance
async def test_status_command_blocked_and_json_exit_2() -> None:
    """blocked=True + --json + --blocked → exit code 2."""
    ser = _MockSerializer()
    payload = _status_response(epoch=4, blocked=True)
    client = _FakeClient(
        Envelope(kind="rebalance.status.response", payload=ser.encode(payload))
    )
    cmd, console, err = _cmd(client)

    code = await cmd.run(json_output=True, blocked_only=True)

    assert code == 2
