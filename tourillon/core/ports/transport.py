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
"""Transport port contracts: handler interfaces, callbacks, and transport errors."""

from typing import Protocol

from tourillon.core.structure.envelope import Envelope


class SendCallback(Protocol):
    """Async callable through which a handler pushes an outgoing Envelope.

    The callback is always provided by the transport layer and must never be
    held beyond the lifetime of the connection that supplied it.
    Implementations must not call it after the connection has been closed;
    doing so raises ConnectionClosedError.
    """

    async def __call__(self, envelope: Envelope) -> None:
        """Send envelope to the remote peer."""
        ...


class ReceiveCallback(Protocol):
    """Async callable through which a handler pulls the next incoming Envelope.

    Returns None when the remote peer has closed the connection cleanly or when
    the connection has been shut down by the local node. A None return is the
    signal for a ConnectionHandler to exit its processing loop and return.
    """

    async def __call__(self) -> Envelope | None:
        """Return the next received Envelope, or None on connection close."""
        ...


class TransportError(Exception):
    """Base class for all transport-layer errors.

    Subclasses represent specific failure modes that callers may catch and
    handle distinctly. Code that does not care about the specific kind of
    failure can catch TransportError to handle all transport problems uniformly.
    """


class ConnectionClosedError(TransportError):
    """Raised when an operation is attempted on an already-closed connection.

    This error surfaces when a handler or caller tries to send an Envelope
    after the underlying connection has been closed or after the peer has
    disconnected. It is a programming error on the caller's side and should
    not be caught silently.
    """


class FrameTooLargeError(TransportError):
    """Raised when a received frame length exceeds the configured maximum.

    The frame_len attribute carries the declared length from the wire so that
    the caller can log it for debugging. The connection must be closed
    immediately after this error is raised, as the receive buffer is now in
    an unrecoverable state.
    """

    def __init__(self, frame_len: int, max_len: int) -> None:
        super().__init__(
            f"received frame declares {frame_len} bytes,"
            f" exceeds maximum of {max_len} bytes"
        )
        self.frame_len: int = frame_len
        self.max_len: int = max_len


class InvalidEnvelopeError(TransportError):
    """Raised when a received frame cannot be decoded into a valid Envelope.

    The cause attribute holds the original exception so that callers can
    inspect the root failure. On receiving this error the connection must be
    closed because the protocol framing state is unknown.
    """

    def __init__(self, cause: Exception) -> None:
        super().__init__(f"failed to decode envelope from frame: {cause}")
        self.cause: Exception = cause


class TlsHandshakeError(TransportError):
    """Raised when the mTLS handshake fails during connection establishment.

    This error is raised both on the server side when an incoming connection
    does not present a valid client certificate and on the client side when the
    server certificate cannot be verified. Any plaintext or unauthenticated
    connection attempt results in this error rather than a silent downgrade.
    """


class KindHandler(Protocol):
    """Async callable that processes a single Envelope of a specific kind.

    A KindHandler is registered with a Dispatcher for one specific kind string.
    It is invoked for every Envelope whose kind matches the registered value.
    The send callback allows the handler to push one or more response Envelopes
    back through the same connection. Handlers must not retain the send callback
    after __call__ returns.

    Any plain async function with the signature
    ``async def f(envelope: Envelope, send: SendCallback) -> None`` satisfies
    this protocol, so handlers do not need to be wrapped in a class. The
    Dispatcher's own handle method also satisfies this protocol, enabling
    hierarchical dispatch: a sub-Dispatcher can be registered as the handler
    for a namespace of kind strings.

    Handlers must not block the event loop; any CPU-bound work must be
    delegated via asyncio.to_thread. Raise TransportError subclasses to signal
    unrecoverable conditions that the Dispatcher should propagate to the
    connection layer. Any other exception is treated as an application-level
    error logged by the Dispatcher, and the loop continues processing subsequent
    envelopes.
    """

    async def __call__(self, envelope: Envelope, send: SendCallback) -> None:
        """Process envelope and optionally respond via send."""
        ...


class ConnectionHandler(Protocol):
    """Per-connection coroutine that drives the full lifecycle of one connection.

    A ConnectionHandler is invoked exactly once per accepted or established
    connection. It receives a receive callable and a send callable that are
    bound to that specific connection. The handler is responsible for running
    the message processing loop: it calls receive() in a loop, processes
    each Envelope, and produces responses via send(). When receive() returns
    None the remote peer has disconnected and the handler must return.

    The ConnectionHandler does not own the transport; it must not close the
    underlying stream directly. Returning from __call__ is sufficient to
    signal to the transport that the connection may be torn down.

    The canonical implementation of this protocol is Dispatcher, which routes
    envelopes to per-kind KindHandlers. Custom implementations are possible
    for specialised uses such as proxy paths or protocol bridges.
    """

    async def __call__(self, receive: ReceiveCallback, send: SendCallback) -> None:
        """Run the message processing loop for one connection.

        This coroutine runs for the lifetime of the connection. It must return
        cleanly when receive() returns None. It may raise TransportError to
        signal that the connection should be torn down immediately due to an
        unrecoverable error. Other exceptions are treated as unexpected and
        will be logged by the transport before the connection is closed.
        """
        ...
