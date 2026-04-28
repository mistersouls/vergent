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

import asyncio
import ssl

import pytest

from tourillon.core.net.tcp.server import TcpServer
from tourillon.core.ports.transport import TransportError


class _DummyWriter:
    def __init__(self) -> None:
        self._closed = False

        class _T:
            def __init__(self):
                self._protocol = None

            def set_protocol(self, p):
                self._protocol = p

            def get_protocol(self):
                return self._protocol

        self.transport = _T()

    def write(self, data: bytes) -> None:
        pass

    async def drain(self) -> None:
        await asyncio.sleep(0)

    def close(self) -> None:
        self._closed = True

    async def wait_closed(self) -> None:
        await asyncio.sleep(0)

    def get_extra_info(self, name, default=None):
        return ("127.0.0.1", 9999)


@pytest.mark.asyncio
async def test_constructor_validates_ssl_context() -> None:
    # None ssl_ctx is forbidden
    with pytest.raises(ValueError):
        TcpServer("127.0.0.1", 0, None, lambda r, s: None)  # type: ignore[arg-type]

    # ssl context must require peer certificates
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.verify_mode = ssl.CERT_NONE
    with pytest.raises(ValueError):
        TcpServer("127.0.0.1", 0, ctx, lambda r, s: None)


@pytest.mark.asyncio
async def test_handle_connection_translates_transport_error_and_closes() -> None:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.verify_mode = ssl.CERT_REQUIRED

    async def handler_raise_transport(receive, send):
        raise TransportError("boom")

    server = TcpServer("127.0.0.1", 0, ctx, handler_raise_transport)
    reader = asyncio.StreamReader()
    reader.feed_eof()
    writer = _DummyWriter()

    # should not raise, and should close writer
    await server._handle_connection(reader, writer)
    assert writer._closed is True


@pytest.mark.asyncio
async def test_handle_connection_logs_on_unexpected_exception_and_closes() -> None:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.verify_mode = ssl.CERT_REQUIRED

    async def handler_raise(receive, send):
        raise RuntimeError("uh-oh")

    server = TcpServer("127.0.0.1", 0, ctx, handler_raise)
    reader = asyncio.StreamReader()
    reader.feed_eof()
    writer = _DummyWriter()

    await server._handle_connection(reader, writer)
    assert writer._closed is True
