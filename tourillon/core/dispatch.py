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
"""Dispatcher — routes incoming Envelopes to per-kind KindHandlers."""

import logging

from tourillon.core.ports.transport import (
    ConnectionHandler,
    KindHandler,
    ReceiveCallback,
    SendCallback,
    TransportError,
)
from tourillon.core.structure.envelope import Envelope

_logger = logging.getLogger(__name__)


class Dispatcher:
    """Core connection handler that routes each Envelope to a registered KindHandler.

    Dispatcher is the canonical implementation of ConnectionHandler. It is
    constructed once and may be shared across all connections served by a
    TcpServer because it holds no per-connection state: the receive and send
    callbacks injected at call time are fully scoped to a single connection.

    Callers register KindHandlers before the server starts accepting connections
    by calling register(kind, handler). Each kind string maps to exactly one
    handler; registering a second handler for the same kind overwrites the
    first, which is intentional and allows run-time reconfiguration of routing
    tables without restarting the server.

    When an Envelope arrives whose kind has no registered handler the Dispatcher
    logs a warning at WARNING level and discards the Envelope. This is a
    deliberate design choice: unrecognised kinds must not crash the connection
    loop, because a future protocol version may introduce new kinds that older
    nodes need to tolerate gracefully during a rolling upgrade.

    Handler exceptions are isolated per-envelope. If a KindHandler raises an
    exception that is not a subclass of TransportError the Dispatcher logs the
    error and continues processing the next Envelope. If the handler raises a
    TransportError the Dispatcher re-raises it immediately, which causes the
    transport layer to close the connection.

    Dispatcher satisfies both ConnectionHandler (via __call__) and KindHandler
    (via handle), enabling hierarchical dispatch: a top-level Dispatcher may
    register a nested Dispatcher's handle method as the handler for a
    sub-namespace of kind strings without any adapter boilerplate.
    """

    def __init__(self) -> None:
        """Initialise an empty routing table."""
        self._handlers: dict[str, KindHandler] = {}

    def register(self, kind: str, handler: KindHandler) -> None:
        """Register handler as the receiver for all Envelopes whose kind matches.

        Raise ValueError when kind is empty. Overwriting an existing
        registration is allowed; the new handler silently replaces the old one.
        This method is not coroutine-safe: all registrations must complete
        before the first connection is accepted by the server.
        """
        if not kind:
            raise ValueError("kind must not be empty")
        if kind in self._handlers:
            _logger.warning(
                "Overwriting existing handler for kind %r with %r", kind, handler
            )
        self._handlers[kind] = handler

    def registered_kinds(self) -> frozenset[str]:
        """Return the set of kind strings that currently have a registered handler."""
        return frozenset(self._handlers)

    async def handle(self, envelope: Envelope, send: SendCallback) -> None:
        """Dispatch a single Envelope to its registered KindHandler.

        This method satisfies the KindHandler callable protocol, allowing a
        Dispatcher to be nested inside another Dispatcher by registering
        this method: ``outer.register("ns", inner_dispatcher.handle)``.
        """
        await self._dispatch_one(envelope, send)

    async def __call__(self, receive: ReceiveCallback, send: SendCallback) -> None:
        """Run the dispatch loop for one connection.

        Pull Envelopes from receive() until it returns None (signalling that
        the peer has disconnected) then return cleanly. For each Envelope
        invoke the registered KindHandler, if any. TransportError raised by a
        handler propagates immediately. All other exceptions from handlers are
        caught, logged, and the loop continues.
        """
        while True:
            envelope: Envelope | None = await receive()
            if envelope is None:
                return
            await self._dispatch_one(envelope, send)

    async def _dispatch_one(self, envelope: Envelope, send: SendCallback) -> None:
        """Route one Envelope to its handler and manage exception isolation.

        If no handler is registered for envelope.kind a WARNING is logged and
        the method returns without raising. TransportError propagates to the
        caller. Any other exception is logged at ERROR level and swallowed so
        that a misbehaving handler does not kill the connection loop.
        """
        handler = self._handlers.get(envelope.kind)
        if handler is None:
            _logger.warning(
                "No handler registered for kind %r (correlation_id=%s),"
                " discarding envelope",
                envelope.kind,
                envelope.correlation_id,
            )
            return
        try:
            await handler(envelope, send)
        except TransportError:
            raise
        except Exception:
            _logger.exception(
                "Unhandled exception in KindHandler for kind %r" " (correlation_id=%s)",
                envelope.kind,
                envelope.correlation_id,
            )


def _assert_protocols() -> None:
    _: ConnectionHandler = Dispatcher()  # type: ignore[assignment]


_assert_protocols()
