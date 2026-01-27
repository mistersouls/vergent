import asyncio
import logging
import ssl
import struct

from vergent.core.config import ReplicationConfig
from vergent.core.model.event import Event
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription

# partition_id length
HEADER_PID = struct.Struct("!I")
# key_len, value_len
HEADER_KV = struct.Struct("!II")


class ReplicationServer:
    """
    Permanent TCP server dedicated to receiving partition key streams
    from peers.

    Each connection can transfer multiple partitions in sequence:
        repeat:
            [4 bytes pid_len][pid bytes]
            repeat:
                [4 bytes key_len][4 bytes value_len][key][value]
            until [0][0]
    until client closes the connection (EOF)
    """

    def __init__(
        self,
        config: ReplicationConfig,
        storage: VersionedStorage,
        subscription: Subscription,
        batch_size: int = 1024,
    ) -> None:
        self._config = config
        self._storage = storage
        self._subscription = subscription
        self._batch_size = batch_size

        self._server: asyncio.AbstractServer | None = None
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_transfers)

        self._logger = logging.getLogger("vergent.core.replication")

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._config.host,
            port=self._config.port,
            ssl=self._config.ssl_ctx,
        )

        addr = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        self._logger.info(f"Replication server listening on {addr}")

    async def shutdown(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._logger.info("Replication server stopped")

    async def _receive_partition(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Receive exactly one partition on an already established connection.
        """
        pid_len_bytes = await reader.readexactly(HEADER_PID.size)
        (pid_len,) = HEADER_PID.unpack(pid_len_bytes)

        pid_bytes = await reader.readexactly(pid_len)
        pid = int(pid_bytes.decode("ascii"), 16)
        self._logger.info(f"Receiving partition {pid}")

        batch: list[tuple[bytes, bytes]] = []

        while True:
            header = await reader.readexactly(HEADER_KV.size)
            key_len, value_len = HEADER_KV.unpack(header)

            if key_len == 0 and value_len == 0:
                break

            key = await reader.readexactly(key_len)
            value = await reader.readexactly(value_len)

            batch.append((key, value))

            if len(batch) >= self._batch_size:
                await self._storage.apply_many(pid_bytes, batch)
                batch.clear()

        if batch:
            await self._storage.apply_many(pid_bytes, batch)

        writer.write(b"OK")
        await writer.drain()
        self._subscription.publish(Event(type="_sync/partition", payload={"pid": pid}))
        self._logger.info(f"Partition {pid} fully received")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        self._logger.debug(f"Incoming replication connection from {peer}")

        async with self._semaphore:
            while True:
                try:
                    await asyncio.wait_for(
                        self._receive_partition(reader, writer),
                        timeout=self._config.timeout_transfers,
                    )
                except asyncio.IncompleteReadError:
                    # client closed the connection (EOF)
                    self._logger.debug(f"Replication connection closed by peer {peer}")
                    break
                except asyncio.TimeoutError as exc:
                    self._logger.warning(
                        f"Replication timeout from {peer}: {exc}"
                    )
                    break
                except Exception as exc:
                    self._logger.error(
                        f"Replication connection error from {peer}: {exc}",
                        exc_info=exc,
                    )
                    break


class PartitionTransfer:
    def __init__(
        self,
        ssl_ctx: ssl.SSLContext,
        storage: VersionedStorage,
        loop: asyncio.AbstractEventLoop,
        max_concurrent: int = 4,
        max_retries: int = 3,
    ) -> None:
        self._ssl_ctx = ssl_ctx
        self._storage = storage
        self._loop = loop

        self._tasks: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._max_retries = max_retries

        self._logger = logging.getLogger("vergent.core.replication")

    def accept(self, address: str, partitions: list[int]) -> None:
        task = self._loop.create_task(self._transfer(address, partitions))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def transfer(self, address: str, partitions: list[int]) -> None:
        async with self._semaphore:
            sender = PartitionSender(address, self._ssl_ctx, self._storage)

            for attempt in range(1, self._max_retries + 1):
                try:
                    await sender.send_partitions(partitions)
                    return
                except Exception as exc:
                    if attempt == self._max_retries:
                        self._logger.error(
                            f"Replication to {address} failed after {attempt} attempts: {exc}",
                            exc_info=exc,
                        )
                        raise
                    self._logger.warning(
                        f"Replication to {address} failed on attempt {attempt}: {exc} – retrying..."
                    )
                    await asyncio.sleep(0.5 * attempt)

    async def shutdown(self) -> None:
        for task in list(self._tasks):
            task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        self._logger.info("PartitionTransfer shutdown complete")

    async def _transfer(self, address: str, partitions: list[int]) -> None:
        try:
            await self.transfer(address, partitions)
        except asyncio.CancelledError:
            self._logger.warning(f"Replication to {address} cancelled")
        except Exception as exc:
            self._logger.error(f"Replication to {address} failed: {exc}", exc_info=exc)


class PartitionSender:
    """
    Sends one or more partitions to a remote ReplicationServer:

        repeat:
            [4 bytes pid_len][pid bytes]
            repeat:
                [4 bytes key_len][4 bytes value_len][key][value]
            until [0][0]
            <wait for "OK">
        until no more partitions, then client closes connection
    """

    def __init__(
        self,
        address: str,
        ssl_ctx: ssl.SSLContext,
        storage: VersionedStorage,
    ) -> None:
        host, port = address.split(":")
        self._host = host
        self._port = int(port)
        self._ssl_ctx = ssl_ctx
        self._storage = storage

        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

        self.connected = False
        self._logger = logging.getLogger("vergent.core.replication")

    async def connect(self) -> None:
        if not self.connected:
            self._reader, self._writer = await asyncio.open_connection(
                host=self._host,
                port=self._port,
                ssl=self._ssl_ctx,
            )
            self.connected = True

    async def close(self) -> None:
        if self._writer:
            self.connected = False
            self._writer.close()
            await self._writer.wait_closed()

    async def send_partition(self, pid: int) -> None:
        await self.connect()
        writer = self._writer
        reader = self._reader

        assert writer is not None
        assert reader is not None

        pid_bytes = format(pid, "x").encode("ascii")

        writer.write(HEADER_PID.pack(len(pid_bytes)))
        writer.write(pid_bytes)
        await writer.drain()

        async for key, value in self._storage.iter_raw(pid_bytes, batch_size=1024):
            writer.write(HEADER_KV.pack(len(key), len(value)))
            writer.write(key)
            writer.write(value)

            if writer.transport.get_write_buffer_size() > 256 * 1024:
                await writer.drain()

        writer.write(HEADER_KV.pack(0, 0))
        await writer.drain()

        ack = await reader.readexactly(2)
        if ack != b"OK":
            raise RuntimeError(f"Invalid ACK from server: {ack!r}")

        self._logger.info(f"Partition {pid} successfully sent")

    async def send_partitions(self, partitions: list[int]) -> None:
        await self.connect()
        try:
            for pid in partitions:
                await self.send_partition(pid)
        finally:
            await self.close()
