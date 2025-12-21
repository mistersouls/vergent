import asyncio
import logging
import struct

import msgpack

from vergent.core.config import Config
from vergent.core.flow import FlowControl
from vergent.core.model.event import Event
from vergent.core.model.state import ServerState
from vergent.core.types_ import GatewayProtocol


class Protocol(asyncio.Protocol):
    def __init__(
        self,
        config: Config,
        server_state: ServerState,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._transport: asyncio.Transport = None   # type: ignore[assignment]
        self._flow: FlowControl = None  # type: ignore[assignment]
        self._cycle: Cycle = None   # type: ignore[assignment]

        self._config = config
        self._app = config.app
        self._loop = loop or asyncio.get_event_loop()
        self._connections = server_state.connections
        self._tasks = server_state.tasks
        self._buffer = bytearray()
        self._expected_length: int | None = None
        self._logger = logging.getLogger("vergent.core.transport")

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        self._flow = FlowControl()
        self._connections.add(self)
        self._cycle = Cycle(
            transport=self._transport,
            flow=self._flow,
            queue=asyncio.Queue()
        )
        task = self._loop.create_task(self._cycle.run_app(self._app))
        task.add_done_callback(self._tasks.discard)
        self._tasks.add(task)

        self._logger.debug(f"Connection made: {self._transport}")

    def connection_lost(self, exc: Exception | None) -> None:
        self._connections.discard(self)
        self._logger.debug(f"Connection lost: {self._transport}")

        if self._flow is not None:
            self._flow.resume_writing()
        if exc is None:
            self._transport.close()

        self._cycle.queue.put_nowait(None)

    def eof_received(self) -> None:
        pass

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)

        if len(self._buffer) > self._config.max_buffer_size:
            self._logger.warning("Buffer overflow, closing connection")
            self._transport.close()
            return

        while True:
            if self._expected_length is None:
                if len(self._buffer) < 4:
                    return

                # "!I" = uint32 big-endian (network order)
                self._expected_length = struct.unpack("!I", self._buffer[:4])[0]
                del self._buffer[:4]

                if self._expected_length > self._config.max_buffer_size:
                    self._logger.warning("Message too large, closing connection")
                    self._transport.close()
                    return

            if len(self._buffer) < self._expected_length:
                return

            payload = self._buffer[:self._expected_length]
            del self._buffer[:self._expected_length]
            self._expected_length = None

            event = self._decode_event(payload)
            if event is None:
                continue

            try:
                self._cycle.queue.put_nowait(event)
            except Exception as exc:
                self._logger.error(f"Queue error: {exc}")
                continue

    def pause_writing(self) -> None:
        self._flow.pause_writing()

    def resume_writing(self) -> None:
        self._flow.resume_writing()

    def _decode_event(self, payload: bytes) -> Event | None:
        try:
            msg = msgpack.unpackb(payload, raw=False)
            return Event(**msg)
        except Exception as exc:
            self._logger.warning(f"Invalid msgpack payload: {exc}")
            return None


class Cycle:
    def __init__(
        self,
        transport: asyncio.Transport,
        flow: FlowControl,
        queue: asyncio.Queue[Event | None]
    ) -> None:
        self.queue = queue
        self._transport = transport
        self._flow = flow
        self._logger = logging.getLogger("vergent.core.transport")

    async def send(self, event: Event) -> None:
        if self._flow.write_paused:
            await self._flow.drain()

        try:
            payload = msgpack.packb(event.to_dict(), use_bin_type=True)
            frame = struct.pack("!I", len(payload)) + payload
            self._transport.write(frame)
        except Exception as exc:
            self._logger.error(f"Failed to send event: {exc}")
            self._transport.close()

    async def receive(self) -> Event:
        return await self.queue.get()

    async def run_app(self, app: GatewayProtocol) -> None:
        try:
            await app(self.receive, self.send)
        except BaseException as exc:
            self._logger.error("Exception in Application gateway", exc_info=exc)
        finally:
            self._transport.close()
