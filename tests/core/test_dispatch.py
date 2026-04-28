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
"""Tests for the core Dispatcher routing component."""

import asyncio
import uuid

import pytest

from tourillon.core.dispatch import Dispatcher
from tourillon.core.ports.transport import (
    SendCallback,
    TransportError,
)
from tourillon.core.structure.envelope import Envelope


def _envelope(kind: str, payload: bytes = b"") -> Envelope:
    """Build a minimal Envelope for use in tests."""
    return Envelope.create(payload, kind=kind, correlation_id=uuid.uuid4())


async def _noop_send(envelope: Envelope) -> None:  # noqa: ARG001
    """Send callback that discards the outgoing envelope."""


def test_register_empty_kind_raises() -> None:
    """Registering an empty kind string must raise ValueError immediately."""
    dispatcher = Dispatcher()
    with pytest.raises(ValueError, match="kind must not be empty"):
        dispatcher.register("", _noop_send)  # type: ignore[arg-type]


def test_register_records_kind() -> None:
    """Registered kind appears in registered_kinds()."""

    async def handler(envelope: Envelope, send: SendCallback) -> None:
        pass

    dispatcher = Dispatcher()
    dispatcher.register("put", handler)
    assert "put" in dispatcher.registered_kinds()


def test_register_overwrite_replaces_handler() -> None:
    """Registering a second handler for the same kind replaces the first."""
    received: list[str] = []

    async def handler_a(envelope: Envelope, send: SendCallback) -> None:
        received.append("a")

    async def handler_b(envelope: Envelope, send: SendCallback) -> None:
        received.append("b")

    dispatcher = Dispatcher()
    dispatcher.register("put", handler_a)
    dispatcher.register("put", handler_b)

    asyncio.run(dispatcher.handle(_envelope("put"), _noop_send))
    assert received == ["b"]


@pytest.mark.asyncio
async def test_dispatch_loop_routes_to_handler() -> None:
    """__call__ routes each received envelope to the matching KindHandler."""
    received: list[Envelope] = []

    async def handler(envelope: Envelope, send: SendCallback) -> None:
        received.append(envelope)

    dispatcher = Dispatcher()
    dispatcher.register("put", handler)

    env = _envelope("put", b"hello")
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    await queue.put(env)
    await queue.put(None)

    async def receive() -> Envelope | None:
        return await queue.get()

    await dispatcher(receive, _noop_send)
    assert received == [env]


@pytest.mark.asyncio
async def test_dispatch_loop_exits_on_none() -> None:
    """The dispatch loop returns cleanly when receive() yields None."""
    dispatcher = Dispatcher()

    async def receive() -> Envelope | None:
        return None

    await dispatcher(receive, _noop_send)


@pytest.mark.asyncio
async def test_dispatch_loop_skips_unknown_kind(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unknown kind produces a WARNING log and the loop continues."""
    import logging

    dispatcher = Dispatcher()
    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    await queue.put(_envelope("unknown.kind"))
    await queue.put(None)

    async def receive() -> Envelope | None:
        return await queue.get()

    with caplog.at_level(logging.WARNING, logger="tourillon.core.dispatch"):
        await dispatcher(receive, _noop_send)

    assert any("unknown.kind" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_dispatch_loop_propagates_transport_error() -> None:
    """A TransportError raised by a handler propagates immediately."""

    async def bad_handler(envelope: Envelope, send: SendCallback) -> None:
        raise TransportError("boom")

    dispatcher = Dispatcher()
    dispatcher.register("put", bad_handler)

    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    await queue.put(_envelope("put"))

    async def receive() -> Envelope | None:
        return await queue.get()

    with pytest.raises(TransportError, match="boom"):
        await dispatcher(receive, _noop_send)


@pytest.mark.asyncio
async def test_dispatch_loop_swallows_non_transport_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-TransportError raised by a handler is logged, loop continues."""
    import logging

    call_count = 0

    async def flaky_handler(envelope: Envelope, send: SendCallback) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("transient failure")

    dispatcher = Dispatcher()
    dispatcher.register("put", flaky_handler)

    queue: asyncio.Queue[Envelope | None] = asyncio.Queue()
    await queue.put(_envelope("put"))
    await queue.put(_envelope("put"))
    await queue.put(None)

    async def receive() -> Envelope | None:
        return await queue.get()

    with caplog.at_level(logging.ERROR, logger="tourillon.core.dispatch"):
        await dispatcher(receive, _noop_send)

    assert call_count == 2
    assert any(
        "Unhandled exception" in rec.message and rec.levelname == "ERROR"
        for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_handle_dispatches_single_envelope() -> None:
    """handle() dispatches one envelope to the matching handler."""
    received: list[Envelope] = []

    async def handler(envelope: Envelope, send: SendCallback) -> None:
        received.append(envelope)

    dispatcher = Dispatcher()
    dispatcher.register("delete", handler)
    env = _envelope("delete", b"payload")
    await dispatcher.handle(env, _noop_send)
    assert received == [env]


@pytest.mark.asyncio
async def test_handle_unknown_kind_does_not_raise() -> None:
    """handle() with an unregistered kind must not raise."""
    dispatcher = Dispatcher()
    await dispatcher.handle(_envelope("ghost"), _noop_send)


@pytest.mark.asyncio
async def test_handle_enables_nested_dispatcher() -> None:
    """A Dispatcher's handle method can be registered in a parent Dispatcher.

    When outer dispatches an envelope with kind 'routed' to inner.handle,
    inner runs its own routing table against the same envelope kind and
    forwards it to inner_handler.
    """
    inner_received: list[Envelope] = []

    async def inner_handler(envelope: Envelope, send: SendCallback) -> None:
        inner_received.append(envelope)

    inner = Dispatcher()
    inner.register("routed", inner_handler)

    outer = Dispatcher()
    outer.register("routed", inner.handle)

    env = _envelope("routed", b"nested")
    await outer.handle(env, _noop_send)
    assert len(inner_received) == 1
    assert inner_received[0].kind == "routed"


@pytest.mark.asyncio
async def test_handler_can_respond_via_send() -> None:
    """A KindHandler may call send() to push a response envelope."""

    async def echo_handler(envelope: Envelope, send: SendCallback) -> None:
        reply = Envelope.create(envelope.payload, kind="put.ack")
        await send(reply)

    sent: list[Envelope] = []

    async def capture_send(envelope: Envelope) -> None:
        sent.append(envelope)

    dispatcher = Dispatcher()
    dispatcher.register("put", echo_handler)
    await dispatcher.handle(_envelope("put", b"data"), capture_send)
    assert len(sent) == 1
    assert sent[0].kind == "put.ack"
    assert sent[0].payload == b"data"
