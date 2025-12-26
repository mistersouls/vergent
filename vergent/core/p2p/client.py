import asyncio
import logging
import ssl
import struct

import msgpack

from vergent.core.model.event import Event
from vergent.core.sub import Subscription
from vergent.core.utils.retry import BackoffRetry


class PeerClient:
    def __init__(
        self,
        address: str,
        ssl_ctx: ssl.SSLContext,
        subscription: Subscription[Event | None],
        loop: asyncio.AbstractEventLoop
    ) -> None:
        self._address = address
        self._ssl_ctx = ssl_ctx
        self._subscription = subscription
        self._loop = loop

        self._reader: asyncio.StreamReader = None   # type: ignore[assignment]
        self._writer: asyncio.StreamWriter = None     # type: ignore[assignment]
        self._receive_task: asyncio.Task[None] | None = None
        self.connected = False
        self._backoff = BackoffRetry()

        self._logger = logging.getLogger("vergent.core.peering")

    async def connect(self) -> None:
        while not self.connected:
            try:
                host, port = self._address.split(":")
                self._reader, self._writer = await asyncio.open_connection(
                    host=host,
                    port=int(port),
                    ssl=self._ssl_ctx,
                    server_hostname=None
                )
                self.connected = True
                if self._receive_task:
                    self._receive_task.cancel()
                self._receive_task = self._loop.create_task(self.receive_loop())
                break
            except Exception as ex:
                delay = self._backoff.next_delay()
                self._logger.warning(
                    f"Connect failed to {self._address}: {ex}. "
                    f"Retrying in {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    async def send(self, event: Event) -> None:
        if not self.connected:
            await self.connect()

        frame = event.to_frame()
        self._writer.write(frame)
        await self._writer.drain()

    async def receive_loop(self) -> None:
        try:
            while True:
                if not self.connected:
                    break

                header = await self._reader.readexactly(4)
                length = struct.unpack("!I", header)[0]
                payload = await self._reader.readexactly(length)
                data = msgpack.unpackb(payload, raw=False)
                if not data:
                    break
                self._subscription.publish(Event(**data))
        except asyncio.IncompleteReadError:
            self._logger.info(f"Peer {self._address} disconnected")
        except Exception as ex:
            self._logger.error(f"Error received for {self._address}: {ex}", exc_info=ex)
        finally:
            await self.close()

    async def close(self) -> None:
        self.connected = False
        if self._receive_task:
            self._receive_task.cancel()
            self._receive_task = None

        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()


class PeerClientPool:
    def __init__(
        self,
        subscription: Subscription[Event | None],
        ssl_ctx: ssl.SSLContext,
        loop: asyncio.AbstractEventLoop
    ) -> None:
        self._subscription = subscription
        self._ssl_ctx = ssl_ctx
        self._loop = loop
        self.clients: dict[str, PeerClient] = {}

    def get(self, address) -> PeerClient:
        if address not in self.clients:
            self.clients[address] = PeerClient(address, self._ssl_ctx, self._subscription, self._loop)
        return self.clients[address]

    async def close(self) -> None:
        await asyncio.gather(*[client.close() for client in self.clients.values()])
