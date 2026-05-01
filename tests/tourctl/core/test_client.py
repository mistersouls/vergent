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
"""Tests for tourctl.core.client — TcpClient error paths and happy path."""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from tourctl.core.client import (
    NodeUnreachableError,
    RequestTimeoutError,
    ServerError,
    TcpClient,
)
from tourillon.core.handlers.kv import KIND_KV_ERR
from tourillon.core.structure.envelope import Envelope


@pytest.mark.asyncio
async def test_request_raises_node_unreachable_on_os_error() -> None:
    with (
        patch(
            "asyncio.open_connection",
            new=AsyncMock(side_effect=OSError("no route")),
        ),
        pytest.raises(NodeUnreachableError),
    ):
        client = TcpClient("127.0.0.1", 7000, None, Mock())  # type: ignore[arg-type]
        await client.request(Envelope.create(b"", kind="ping"))


@pytest.mark.asyncio
async def test_request_raises_timeout_on_timeout_error() -> None:
    fake_conn = Mock()
    fake_conn.send = AsyncMock()
    fake_conn.receive = AsyncMock(side_effect=TimeoutError())
    fake_conn.close = AsyncMock()

    with (
        patch("asyncio.open_connection", new=AsyncMock(return_value=(None, None))),
        patch("tourctl.core.client.Connection", new=lambda r, w: fake_conn),
        pytest.raises(RequestTimeoutError) as exc,
    ):
        client = TcpClient("127.0.0.1", 7000, None, Mock(), timeout=3.5)  # type: ignore[arg-type]
        await client.request(Envelope.create(b"", kind="ping"))
    assert isinstance(exc.value.timeout, float)
    assert exc.value.timeout == 3.5


@pytest.mark.asyncio
async def test_request_raises_server_error_on_kv_err_kind() -> None:
    fake_conn = Mock()
    fake_conn.send = AsyncMock()
    response = Envelope.create(b"resp", kind=KIND_KV_ERR)
    fake_conn.receive = AsyncMock(return_value=response)
    fake_conn.close = AsyncMock()

    serializer = Mock()
    serializer.decode = Mock(return_value={"error": "boom"})

    with (
        patch("asyncio.open_connection", new=AsyncMock(return_value=(None, None))),
        patch("tourctl.core.client.Connection", new=lambda r, w: fake_conn),
        pytest.raises(ServerError) as exc,
    ):
        client = TcpClient("127.0.0.1", 7000, None, serializer)  # type: ignore[arg-type]
        await client.request(Envelope.create(b"", kind="ping"))
    assert exc.value.message == "boom"


@pytest.mark.asyncio
async def test_request_raises_value_error_when_response_is_none() -> None:
    fake_conn = Mock()
    fake_conn.send = AsyncMock()
    fake_conn.receive = AsyncMock(return_value=None)
    fake_conn.close = AsyncMock()

    with (
        patch("asyncio.open_connection", new=AsyncMock(return_value=(None, None))),
        patch("tourctl.core.client.Connection", new=lambda r, w: fake_conn),
        pytest.raises(ValueError),
    ):
        client = TcpClient("127.0.0.1", 7000, None, Mock())  # type: ignore[arg-type]
        await client.request(Envelope.create(b"", kind="ping"))


@pytest.mark.asyncio
async def test_request_returns_response_on_success() -> None:
    fake_conn = Mock()
    fake_conn.send = AsyncMock()
    response = Envelope.create(b"ok", kind="kv.put.ok")
    fake_conn.receive = AsyncMock(return_value=response)
    fake_conn.close = AsyncMock()

    with (
        patch("asyncio.open_connection", new=AsyncMock(return_value=(None, None))),
        patch("tourctl.core.client.Connection", new=lambda r, w: fake_conn),
    ):
        client = TcpClient("127.0.0.1", 7000, None, Mock())  # type: ignore[arg-type]
        got = await client.request(Envelope.create(b"", kind="ping"))
    assert got is response


@pytest.mark.asyncio
async def test_request_timeout_attribute_carries_configured_value() -> None:
    fake_conn = Mock()
    fake_conn.send = AsyncMock()
    fake_conn.receive = AsyncMock(side_effect=TimeoutError())
    fake_conn.close = AsyncMock()

    with (
        patch("asyncio.open_connection", new=AsyncMock(return_value=(None, None))),
        patch("tourctl.core.client.Connection", new=lambda r, w: fake_conn),
        pytest.raises(RequestTimeoutError) as exc,
    ):
        client = TcpClient("127.0.0.1", 7000, None, Mock(), timeout=2.25)  # type: ignore[arg-type]
        await client.request(Envelope.create(b"", kind="ping"))
    assert exc.value.timeout == 2.25
