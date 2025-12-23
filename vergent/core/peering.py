import asyncio
import logging
import struct

import msgpack

from vergent.core.model.event import Event
from vergent.core.utils.retry import BackoffRetry


class Subscription[T]:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.waiter = self._loop.create_future()

    def publish(self, value: T):
        waiter = self.waiter
        self.waiter = self._loop.create_future()
        waiter.set_result((value, self.waiter))

    async def subscribe(self):
        waiter = self.waiter
        while True:
            value, waiter = await waiter
            yield value

    __aiter__ = subscribe


class PeerClient:
    def __init__(self, address: str, loop: asyncio.AbstractEventLoop) -> None:
        self._address = address
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
                self._reader, self._writer = await asyncio.open_connection(host, int(port))
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

    async def on_event(self, event: Event) -> None:
        # todo(souls): manage later
        self._logger.debug(f"Received event: {event}")

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
                await self.on_event(Event(**data))
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
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self.clients: dict[str, PeerClient] = {}

    def get(self, address) -> PeerClient:
        if address not in self.clients:
            self.clients[address] = PeerClient(address, self._loop)
        return self.clients[address]

    async def close(self) -> None:
        await asyncio.gather(*[client.close() for client in self.clients.values()])


class PeerManager:
    def __init__(
        self,
        listen: str,
        peers: set[str],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._listen = listen
        self._peers = set(peers)
        self._loop = loop

        self._subscription: Subscription[Event | None] = Subscription(loop)
        self._clients = PeerClientPool(loop)
        self._tasks: list[asyncio.Task[None]] = []

        self._logger = logging.getLogger("vergent.core.peering")

    async def manage(self, stop: asyncio.Event) -> None:
        self._logger.info(f"Starting with {len(self._peers)} peers: {sorted(self._peers)}")
        tasks = [
            asyncio.create_task(self._ping(stop)),
            asyncio.create_task(stop.wait()),
        ]
        self._logger.info(f"Started regular ping: {sorted(self._peers)}")
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        self._logger.info("Stop signal received, shutting down...")

        self._subscription.publish(None)
        await asyncio.sleep(0)

        for task in done:
            if exc := task.exception():
                self._logger.error("Exception occurred", exc_info=exc)

        for task in pending:
            task.cancel()

        if pending:
            await asyncio.gather(*pending)


    async def broadcast(self, event: Event) -> None:
        self._subscription.publish(event)

    async def _ping(self, stop: asyncio.Event) -> None:
        tasks = [
            asyncio.create_task(self._ping_one(stop, peer))
            for peer in self._peers
        ]
        self._tasks.extend(tasks)

        event = Event(type="ping", payload={"source": self._listen})
        while not stop.is_set():
            await asyncio.sleep(2)
            self._subscription.publish(event)

    async def _ping_one(self, stop: asyncio.Event, address: str) -> None:
        async for event in self._subscription:
            client = self._clients.get(address)

            if stop.is_set() or event is None:
                await client.close()
                break

            await client.send(event)
            self._logger.debug(f"Sent event: {event}")
