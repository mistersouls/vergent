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
"""Framed Envelope transport over asyncio StreamReader/StreamWriter.

Each Envelope is transmitted as a length-prefixed frame:

    +---------+-----------+
    | len: 4B | body: NB  |
    +---------+-----------+

The 4-byte big-endian unsigned integer at the start of every frame declares
the byte length of the body that follows. The body is a wire-encoded Envelope.
The maximum frame body size is MAX_FRAME_BYTES (4 MiB by default). Frames that
exceed this limit are rejected with FrameTooLargeError and the connection must
be closed immediately because the receive buffer state is unknown.

Backpressure is handled by the asyncio Streams API: StreamWriter.drain()
suspends the sender coroutine until the transport's write buffer drops below
the high-water mark set by asyncio. No separate flow-control shim is required.
"""

import asyncio
import contextlib
import logging
import struct

from tourillon.core.ports.transport import (
    ConnectionClosedError,
    FrameTooLargeError,
    InvalidEnvelopeError,
)
from tourillon.core.structure.envelope import Envelope

_logger = logging.getLogger(__name__)

MAX_FRAME_BYTES: int = 4 * 1024 * 1024  # 4 MiB

_HEADER_FORMAT: str = "!I"
_HEADER_SIZE: int = struct.calcsize(_HEADER_FORMAT)


class Connection:
    """Bidirectional, framed Envelope channel over a pair of asyncio streams.

    Connection wraps a StreamReader/StreamWriter pair and exposes two async
    methods, receive() and send(), that satisfy the ReceiveCallback and
    SendCallback protocols defined in core.ports.transport.

    Framing uses a 4-byte big-endian length prefix followed by the wire-encoded
    Envelope body. This keeps frames self-delimiting without relying on
    newlines or sentinel bytes.

    Backpressure is delegated to asyncio.StreamWriter.drain(), which suspends
    the send() coroutine whenever the transport's write buffer exceeds its
    high-water mark. This is the idiomatic asyncio Streams API mechanism and
    requires no additional protocol shim.

    Connection does not own the lifecycle of the streams. The caller (TcpServer)
    is responsible for closing the writer after the ConnectionHandler returns.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        max_frame_bytes: int = MAX_FRAME_BYTES,
    ) -> None:
        """Attach to an existing stream pair.

        Parameters:
            reader: The asyncio.StreamReader for this connection.
            writer: The asyncio.StreamWriter for this connection.
            max_frame_bytes: Maximum allowed frame body size in bytes. Frames
                whose declared length exceeds this value cause FrameTooLargeError.
        """
        self._reader = reader
        self._writer = writer
        self._max_frame_bytes = max_frame_bytes
        self._closed = False

    async def receive(self) -> Envelope | None:
        """Read and decode the next framed Envelope from the stream.

        Return None when the peer has closed the connection cleanly (EOF) or
        when an incomplete header arrives at EOF. Raise FrameTooLargeError if
        the declared frame length exceeds the configured maximum. Raise
        InvalidEnvelopeError if the frame body cannot be decoded.

        This method satisfies the ReceiveCallback protocol.
        """
        try:
            header = await self._reader.readexactly(_HEADER_SIZE)
        except asyncio.IncompleteReadError as exc:
            if exc.partial == b"":
                return None
            _logger.warning(
                "Incomplete frame header (%d bytes received, expected %d)",
                len(exc.partial),
                _HEADER_SIZE,
            )
            return None

        (frame_len,) = struct.unpack(_HEADER_FORMAT, header)

        if frame_len > self._max_frame_bytes:
            raise FrameTooLargeError(frame_len, self._max_frame_bytes)

        try:
            body = await self._reader.readexactly(frame_len)
        except asyncio.IncompleteReadError:
            _logger.warning("Incomplete frame body (declared %d bytes)", frame_len)
            return None

        try:
            return Envelope.decode(body)
        except Exception as exc:
            raise InvalidEnvelopeError(exc) from exc

    async def send(self, envelope: Envelope) -> None:
        """Encode and write envelope as a framed message to the stream.

        Backpressure is applied naturally by awaiting drain() after each write.
        Raise ConnectionClosedError if this connection has already been closed.

        This method satisfies the SendCallback protocol.

        Parameters:
            envelope: The Envelope to transmit.

        Raises:
            ConnectionClosedError: If the connection is already closed.
        """
        if self._closed:
            raise ConnectionClosedError("send called on a closed connection")

        body = envelope.encode()
        header = struct.pack(_HEADER_FORMAT, len(body))
        self._writer.write(header + body)
        await self._writer.drain()

    async def close(self) -> None:
        """Flush any pending writes and close the underlying stream.

        Calling close() more than once is safe; subsequent calls are no-ops.
        """
        if self._closed:
            return
        self._closed = True
        with contextlib.suppress(Exception):
            await self._writer.drain()
        self._writer.close()
        with contextlib.suppress(Exception):
            await self._writer.wait_closed()

    @property
    def peer_address(self) -> tuple[str, int] | None:
        """Return the (host, port) of the remote peer, or None if unavailable."""
        extra = self._writer.get_extra_info("peername")
        if extra and len(extra) >= 2:
            return str(extra[0]), int(extra[1])
        return None
