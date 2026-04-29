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
"""mTLS TCP client for a single Tourillon node.

TcpClient opens a short-lived mTLS connection per request: connect, send one
Envelope, receive one response, close. This keeps the client stateless and
avoids connection lifecycle complexity, at the cost of one TLS handshake per
operation — acceptable for a CLI tool where latency is human-scale.

Error taxonomy:

- ``NodeUnreachableError`` — the TCP connection could not be established
  (node down, wrong address, firewall).
- ``RequestTimeoutError`` — the node did not respond within the configured
  deadline.
- ``ServerError`` — the node responded with a ``kv.err`` envelope; the
  server-side error message is preserved in ``ServerError.message``.
- ``ValueError`` — the response envelope could not be decoded.

All errors are raised from ``TcpClient.request`` so callers have a single
point at which to handle the full failure space.
"""

import asyncio
import logging
import ssl

from tourillon.bootstrap.handlers import KIND_KV_ERR
from tourillon.core.net.tcp.connection import Connection
from tourillon.core.ports.serializer import SerializerPort
from tourillon.core.structure.envelope import Envelope

_logger = logging.getLogger(__name__)


class NodeUnreachableError(Exception):
    """Raised when a TCP connection to the target node cannot be established.

    Callers should display the host and port to help the operator diagnose the
    problem — wrong address, node not started, or network partition.
    """


class RequestTimeoutError(Exception):
    """Raised when the node does not respond within the configured deadline.

    The ``timeout`` attribute carries the limit in seconds so that callers can
    include it in the error message without repeating the value.
    """

    def __init__(self, timeout: float) -> None:
        super().__init__(f"request timed out after {timeout}s")
        self.timeout = timeout


class ServerError(Exception):
    """Raised when the node responds with a ``kv.err`` envelope.

    The ``message`` attribute contains the error string decoded from the
    server-side payload. Callers should display it verbatim so that the
    operator sees the exact diagnostic produced by the node.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class TcpClient:
    """Short-lived mTLS TCP client for one Tourillon node.

    Each call to ``request`` opens a fresh connection, sends the provided
    Envelope, waits for the server response, and closes the connection before
    returning. The client enforces mTLS: both a client certificate/key pair and
    a CA bundle are mandatory; any attempt to construct a client-side context
    without them raises ``TlsConfigurationError`` from the ``build_ssl_context``
    factory before this class is even instantiated.

    A single TcpClient instance may be called multiple times but does not pool
    connections — each ``request`` call incurs a full TCP + TLS handshake.
    """

    def __init__(
        self,
        host: str,
        port: int,
        ssl_ctx: ssl.SSLContext,
        serializer: SerializerPort,
        *,
        timeout: float = 10.0,
    ) -> None:
        """Configure the client for a specific node endpoint.

        Parameters:
            host: Hostname or IP address of the target Tourillon node.
            port: TCP port of the target node.
            ssl_ctx: A client-side SSLContext built with ``build_ssl_context``
                (``server_side=False``). Must require peer certificates.
            serializer: Used to decode ``kv.err`` payloads for ``ServerError``.
            timeout: Per-request deadline in seconds. Applies to the combined
                send + receive window.
        """
        self._host = host
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._serializer = serializer
        self._timeout = timeout

    async def request(self, envelope: Envelope) -> Envelope:
        """Send envelope to the node and return the response Envelope.

        Opens a new mTLS connection, sends envelope, awaits exactly one
        response frame, then closes the connection. The full send + receive
        window is bounded by ``self._timeout``.

        Raises:
            NodeUnreachableError: If the TCP connection cannot be established.
            RequestTimeoutError: If no response arrives within the timeout.
            ServerError: If the response kind is ``kv.err``.
            ValueError: If the response frame cannot be decoded.
        """
        conn: Connection | None = None
        try:
            try:
                reader, writer = await asyncio.open_connection(
                    self._host, self._port, ssl=self._ssl_ctx
                )
            except OSError as exc:
                _logger.debug("node unreachable %s:%d: %s", self._host, self._port, exc)
                raise NodeUnreachableError(
                    f"cannot reach {self._host}:{self._port}"
                ) from exc

            conn = Connection(reader, writer)

            try:
                async with asyncio.timeout(self._timeout):
                    await conn.send(envelope)
                    response = await conn.receive()
            except TimeoutError as exc:
                _logger.debug("request to %s:%d timed out", self._host, self._port)
                raise RequestTimeoutError(self._timeout) from exc

            if response is None:
                raise ValueError(
                    "server closed the connection without sending a response"
                )

            if response.kind == KIND_KV_ERR:
                try:
                    payload = self._serializer.decode(response.payload)
                    message = str(payload.get("error", "server reported an error"))
                except Exception:
                    message = "server reported an error (payload unreadable)"
                raise ServerError(message)

            return response

        finally:
            if conn is not None:
                try:
                    await conn.close()
                except Exception:
                    _logger.debug(
                        "error closing connection to %s:%d", self._host, self._port
                    )
