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
"""In-process transport double for testing ConnectionHandlers without real sockets.

MemoryConnection implements the same ReceiveCallback and SendCallback interfaces
as Connection but operates entirely in memory using asyncio.Queue. It is the
transport-layer equivalent of MemoryStore and MemoryLog: a production-quality
test double that lets callers exercise ConnectionHandler and KindHandler logic
with no network code, no TLS setup, and no OS resources.

Typical usage in a test::

    conn = MemoryConnection()
    await conn.push(Envelope.create(b"", kind="ping"))
    await conn.close_inbound()

    dispatcher = Dispatcher()
    dispatcher.register("ping", ping_handler)
    await dispatcher(conn.receive, conn.send)

    assert conn.sent[0].kind == "pong"
"""

import asyncio

from tourillon.core.ports.transport import ConnectionClosedError
from tourillon.core.structure.envelope import Envelope


class MemoryConnection:
    """Queue-backed, in-process connection double.

    MemoryConnection exposes receive() and send() callables that satisfy the
    ReceiveCallback and SendCallback protocols. Inbound envelopes are fed via
    push(); outbound envelopes written by the handler under test are captured in
    sent. Calling close_inbound() places a None sentinel in the inbound queue,
    which causes receive() to return None and signals the handler to exit its
    loop cleanly.

    The double is intentionally symmetric: two MemoryConnections can be wired
    together for peer-to-peer tests by routing each side's send() to the other
    side's inbound queue.

    MemoryConnection is not thread-safe. It is designed for use within a single
    asyncio event loop.
    """

    def __init__(self) -> None:
        """Initialise an open, empty in-process connection."""
        self._inbound: asyncio.Queue[Envelope | None] = asyncio.Queue()
        self._outbound: list[Envelope] = []
        self._closed = False

    async def receive(self) -> Envelope | None:
        """Return the next inbound Envelope, or None when the inbound side is closed.

        Blocks until an envelope or the None sentinel is available. This method
        satisfies the ReceiveCallback protocol.
        """
        return await self._inbound.get()

    async def send(self, envelope: Envelope) -> None:
        """Capture an outbound Envelope sent by the handler under test.

        Raise ConnectionClosedError when the connection has been closed via
        close(). This method satisfies the SendCallback protocol.

        Parameters:
            envelope: The Envelope produced by the handler.

        Raises:
            ConnectionClosedError: If close() has been called.
        """
        if self._closed:
            raise ConnectionClosedError("send called on a closed MemoryConnection")
        self._outbound.append(envelope)

    async def push(self, envelope: Envelope) -> None:
        """Feed an Envelope into the inbound queue to be consumed by receive().

        Call this before or during the handler's execution to simulate incoming
        messages from the remote peer.

        Parameters:
            envelope: The Envelope to deliver to the handler.
        """
        await self._inbound.put(envelope)

    async def close_inbound(self) -> None:
        """Place the EOF sentinel in the inbound queue.

        After this call, the next receive() returns None, which signals a
        ConnectionHandler to exit its processing loop cleanly.
        """
        await self._inbound.put(None)

    async def close(self) -> None:
        """Mark the connection closed and signal EOF on the inbound side.

        Subsequent calls to send() raise ConnectionClosedError. If the inbound
        side is not already closed, a None sentinel is queued so that any
        pending receive() unblocks.
        """
        if self._closed:
            return
        self._closed = True
        await self._inbound.put(None)

    @property
    def sent(self) -> list[Envelope]:
        """Return a snapshot of all Envelopes captured by send() so far."""
        return list(self._outbound)

    def clear_sent(self) -> None:
        """Discard all captured outbound Envelopes.

        Useful when a test needs to assert on responses from a specific point
        in the interaction without earlier messages polluting the list.
        """
        self._outbound.clear()
