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
"""TcpServer — accepts mTLS connections and drives them through a ConnectionHandler.

TcpServer wraps asyncio.start_server and owns the lifecycle of every accepted
connection. For each new connection it constructs a Connection (which handles
framing and backpressure), then spawns a supervised task inside an
asyncio.TaskGroup that calls the provided ConnectionHandler.

Lifecycle:
    server = TcpServer(host, port, ssl_ctx, handler)
    await server.start()   # begins accepting
    ...
    await server.stop()    # drains in-flight connections, then closes

The server does not start accepting until start() is called. Calling stop()
signals all active connection tasks to finish; it waits for them to complete
before returning so that no connection is left in an undefined state.

mTLS is enforced by the ssl_ctx parameter. The ssl_ctx must have been built
with build_ssl_context(server_side=True). Passing a non-TLS context or None
raises ValueError at construction time so that plaintext nodes can never
accidentally join the cluster.
"""

import asyncio
import logging
import ssl

from tourillon.core.net.tcp.connection import Connection
from tourillon.core.ports.transport import ConnectionHandler, TransportError

_logger = logging.getLogger(__name__)


class TcpServer:
    """Leaderless TCP server that accepts mTLS connections and routes Envelopes.

    Each accepted connection is wrapped in a Connection instance and handed to
    the provided ConnectionHandler. Every connection runs inside a task that is
    supervised by the server's internal TaskGroup, so no orphan tasks are
    created. If a ConnectionHandler raises TransportError the connection is
    closed and the error is logged at WARNING level; unexpected exceptions are
    logged at ERROR level before the connection is torn down.

    The server is shareable: the same TcpServer instance may be started and
    stopped once. Restarting is not supported; construct a new instance instead.
    """

    def __init__(
        self,
        host: str,
        port: int,
        ssl_ctx: ssl.SSLContext,
        handler: ConnectionHandler,
        *,
        max_frame_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        """Configure a TcpServer.

        Parameters:
            host: The interface address to bind to.
            port: The TCP port to listen on.
            ssl_ctx: A server-side SSLContext built with build_ssl_context().
                Must not be None and must require peer certificates.
            handler: The ConnectionHandler invoked for every accepted connection.
            max_frame_bytes: Maximum Envelope frame size forwarded to Connection.

        Raises:
            ValueError: If ssl_ctx is None or does not require peer certificates,
                preventing accidental plaintext or unauthenticated deployments.
        """
        if ssl_ctx is None:
            raise ValueError(
                "ssl_ctx must not be None — plaintext connections are forbidden"
            )
        if ssl_ctx.verify_mode is not ssl.CERT_REQUIRED:
            raise ValueError(
                "ssl_ctx.verify_mode must be ssl.CERT_REQUIRED — "
                "unauthenticated connections are forbidden"
            )
        self._host = host
        self._port = port
        self._ssl_ctx = ssl_ctx
        self._handler = handler
        self._max_frame_bytes = max_frame_bytes
        self._server: asyncio.Server | None = None
        self._stop_event = asyncio.Event()
        self._active_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Bind to the configured address and begin accepting connections.

        This coroutine returns as soon as the server socket is bound. Accepted
        connections are handled concurrently in background tasks that remain
        under this server's supervision.

        Raises:
            RuntimeError: If start() is called more than once on the same instance.
        """
        if self._server is not None:
            raise RuntimeError("TcpServer.start() called more than once")

        self._server = await asyncio.start_server(
            self._on_client_connected,
            host=self._host,
            port=self._port,
            ssl=self._ssl_ctx,
        )
        addr = (
            self._server.sockets[0].getsockname()
            if self._server.sockets
            else (self._host, self._port)
        )
        _logger.info("TcpServer listening on %s:%s (mTLS)", addr[0], addr[1])

    async def stop(self) -> None:
        """Stop accepting new connections and wait for active ones to finish.

        Closes the listening socket first, then waits for all in-flight
        connection tasks to complete so that no connection handler is left
        running after this method returns.
        """
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        self._active_tasks.clear()
        _logger.info("TcpServer stopped")

    async def _on_client_connected(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Callback invoked by asyncio for each accepted connection.

        Spawns a supervised task that runs the connection handler. The task
        removes itself from the active-tasks set when it finishes.
        """
        task: asyncio.Task[None] = asyncio.get_running_loop().create_task(
            self._handle_connection(reader, writer)
        )
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Manage the full lifecycle of one accepted connection.

        Constructs the Connection framing layer, invokes the ConnectionHandler,
        and ensures the writer is always closed regardless of how the handler
        exits. TransportError is logged at WARNING; other exceptions at ERROR.

        mTLS authentication is enforced entirely by the ssl_ctx passed at
        construction time (verify_mode=CERT_REQUIRED). Unauthenticated peers
        are rejected by the TLS handshake before this method is ever called;
        no application-layer certificate check is required here.
        """
        conn = Connection(reader, writer, max_frame_bytes=self._max_frame_bytes)
        peer = conn.peer_address
        _logger.debug("Connection accepted from %s", peer)
        try:
            await self._handler(conn.receive, conn.send)
        except TransportError as exc:
            _logger.warning("Transport error on connection from %s: %s", peer, exc)
        except Exception:
            _logger.exception("Unexpected error on connection from %s", peer)
        finally:
            await conn.close()
            _logger.debug("Connection closed for %s", peer)
