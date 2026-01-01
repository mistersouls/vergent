import asyncio
import logging
import uuid

from vergent.core.model.event import Event
from vergent.core.model.vnode import VNode
from vergent.core.p2p.client import PeerClientPool
from vergent.core.p2p.conflict import ValueVersion, ConflictResolver
from vergent.core.placement import PlacementStrategy
from vergent.core.ring import Ring
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription


class PendingWrite:
    def __init__(
        self,
        request_id: str,
        quorum_write: int,
        loop: asyncio.AbstractEventLoop
    ) -> None:
        self.request_id = request_id
        self.quorum_write = quorum_write
        self.success = 0
        self.failed = 0
        self.future: asyncio.Future[bool] = loop.create_future()

    def ack_success(self) -> None:
        if not self.future.done():
            self.success += 1
            if self.success >= self.quorum_write:
                self.future.set_result(True)

    def ack_failure(self) -> None:
        if not self.future.done():
            self.failed += 1


class PendingRead:
    def __init__(self, request_id: str, quorum_read: int, loop: asyncio.AbstractEventLoop):
        self.request_id = request_id
        self.quorum_read = quorum_read
        self.responses: list[ValueVersion] = []
        self.failed = 0
        self.future: asyncio.Future[ValueVersion | None] = loop.create_future()

    def ack_success(self, version: ValueVersion) -> None:
        if self.future.done():
            return

        self.responses.append(version)

        if len(self.responses) >= self.quorum_read:
            winner = ConflictResolver.resolve(self.responses)
            self.future.set_result(winner)

    def ack_failure(self) -> None:
        if self.future.done():
            return
        self.failed += 1


class Coordinator:
    def __init__(
        self,
        node_id: str,
        ring: Ring,
        peers: PeerClientPool,
        placement: PlacementStrategy,
        subscription: Subscription[Event | None],
        storage: VersionedStorage,
        replication_factor: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._local_node_id = node_id
        self._ring = ring
        self._peers = peers
        self._placement = placement
        self._subscription = subscription
        self._storage = storage
        self._replication_factor = replication_factor
        self._loop = loop

        self._logger = logging.getLogger("vergent.core.coordinator")

        self._pending: dict[str, PendingWrite | PendingRead] = {}
        self._event_task = self._loop.create_task(self._event_loop())
        self._background_tasks: set[asyncio.Task] = set()

    def on_event(self, event: Event) -> None:
        payload = event.payload
        request_id = payload.get("request_id")

        if request_id and event.type in ("ok", "ko"):
            pending = self._pending.get(request_id)

            if not pending:
                return # late

            if isinstance(pending, PendingWrite) and event.type== "ok":
                pending.ack_success()
            elif isinstance(pending, PendingRead) and event.type== "ok":
                pending.ack_success(payload.get("version"))
            else:
                pending.ack_failure()

    async def coordinate_delete(
        self,
        key: bytes,
        primary: VNode,
        quorum_write: int,
        timeout: float,
    ) -> bool:
        replicas = self._ring.preference_list(primary, self._replication_factor)

        request_id = str(uuid.uuid4())
        pending = PendingWrite(request_id, quorum_write, self._loop)
        self._pending[request_id] = pending

        local_version = await self._delete_local(pending, key)
        if local_version is None:
            self._pending.pop(request_id, None)
            return False

        replicate_event = Event(
            type="replicate",
            payload={
                "key": key,
                "version": local_version.to_dict(),
                "request_id": request_id,
            },
        )

        for replica in replicas:
            if replica.node_id == self._local_node_id:
                continue
            transport = self._peers.get(replica.node_id)
            task = self._loop.create_task(transport.send(replicate_event))
            self._track_task(task)

        return await self._write_ack(pending, request_id, timeout)

    async def coordinate_get(
        self,
        key: bytes,
        primary: VNode,
        quorum_read: int,
        timeout: float,
    ) -> bytes | None:

        replicas = self._ring.preference_list(primary, self._replication_factor)

        request_id = str(uuid.uuid4())
        pending = PendingRead(request_id, quorum_read, self._loop)
        self._pending[request_id] = pending

        local_version = await self._storage.get_version(key)
        if local_version:
            pending.ack_success(local_version)

        read_event = Event(
            type="get",
            payload={
                "key": key,
                "request_id": request_id,
            },
        )

        for replica in replicas:
            if replica.node_id != self._local_node_id:
                transport = self._peers.get(replica.node_id)
                task = self._loop.create_task(transport.send(read_event))
                self._track_task(task)

        winner = await self._read_ack(pending, request_id, timeout)

        # Read-repair
        if local_version and winner and winner.hlc > local_version.hlc:
            await self._storage.apply_remote_version(key, winner)

        return None if (winner is None or winner.is_tombstone) else winner.value

    async def coordinate_put(
        self,
        key: bytes,
        value: bytes,
        primary: VNode,
        quorum_write: int,
        timeout: float
    ) -> bool:
        replicas = self._ring.preference_list(primary, self._replication_factor)

        request_id = str(uuid.uuid4())
        pending = PendingWrite(request_id, quorum_write, self._loop)
        self._pending[request_id] = pending

        local_version = await self._put_local(pending, key, value)
        if local_version is None:
            self._pending.pop(request_id, None)
            return False

        replicate_event = Event(
            type="replicate",
            payload={"key": key, "version": local_version}
        )
        for replica in replicas:
            if replica.node_id != self._local_node_id:
                transport = self._peers.get(replica.node_id)
                self._track_task(asyncio.create_task(transport.send(replicate_event)))

        return await self._write_ack(pending, request_id, timeout)

        # Hinted hand-off later

    async def forward_delete(
        self,
        primary: VNode,
        key: bytes,
        quorum_write: int,
        timeout: float,
    ) -> bool:
        request_id = str(uuid.uuid4())
        pending = PendingWrite(request_id, quorum_write, self._loop)
        self._pending[request_id] = pending

        payload = {
            "kind": "delete",
            "key": key,
            "W": quorum_write,
            "request_id": request_id,
        }
        event = Event(type="forward", payload=payload)

        transport = self._peers.get(primary.node_id)
        await transport.send(event)
        return await self._write_ack(pending, request_id, timeout)

    async def forward_get(
        self,
        primary: VNode,
        key: bytes,
        quorum_read: int,
        timeout: float,
    ) -> bytes | None:

        request_id = str(uuid.uuid4())
        pending = PendingRead(request_id, quorum_read, self._loop)
        self._pending[request_id] = pending

        payload = {
            "kind": "get",
            "key": key,
            "R": quorum_read,
            "request_id": request_id,
        }
        event = Event(type="forward", payload=payload)

        transport = self._peers.get(primary.node_id)
        await transport.send(event)
        winner = await self._read_ack(pending, request_id, timeout)

        return None if winner.is_tombstone else winner.value

    async def forward_put(
        self,
        primary: VNode,
        key: bytes,
        value: bytes,
        quorum_write: int,
        timeout: float
    ) -> bool:
        payload = {
            "kind": "put",
            "key": key,
            "value": value,
            "W": quorum_write,
        }
        event = Event(type="forward", payload=payload)
        transport = self._peers.get(primary.node_id)
        request_id = str(uuid.uuid4())
        pending = PendingWrite(request_id, quorum_write, self._loop)
        self._pending[request_id] = pending
        await transport.send(event)
        return await self._write_ack(pending, request_id, timeout)

    async def get(
        self,
        key: bytes,
        quorum_read: int,
        timeout: float,
    ) -> bytes | None:

        partition = self._placement.find_partition_by_key(key)
        primary = self._placement.find_vnode_by_partition(partition)

        if self._local_node_id != primary.node_id:
            return await self.forward_get(
                primary,
                key,
                quorum_read,
                timeout,
            )

        return await self.coordinate_get(
            key,
            primary,
            quorum_read,
            timeout,
        )

    async def put(
        self,
        key: bytes,
        value: bytes,
        quorum_write: int,
        timeout: float
    ) -> bool:
        """
        replication_factor (N)
        quorum_write (W)
        """
        partition = self._placement.find_partition_by_key(key)
        primary = self._placement.find_vnode_by_partition(partition)

        if self._local_node_id != primary.node_id:
            return await self.forward_put(
                primary,
                key,
                value,
                quorum_write,
                timeout
            )

        return await self.coordinate_put(
            key,
            value,
            primary,
            quorum_write,
            timeout
        )

    async def delete(
        self,
        key: bytes,
        quorum_write: int,
        timeout: float,
    ) -> bool:
        """
        replication_factor (N)
        quorum_write (W)
        """
        partition = self._placement.find_partition_by_key(key)
        primary = self._placement.find_vnode_by_partition(partition)

        if self._local_node_id != primary.node_id:
            return await self.forward_delete(
                primary,
                key,
                quorum_write,
                timeout,
            )

        return await self.coordinate_delete(
            key,
            primary,
            quorum_write,
            timeout,
        )

    async def close(self):
        # 1. Stop event loop
        self._event_task.cancel()
        try:
            await self._event_task
        except asyncio.CancelledError:
            pass

        # 2. Cancel background tasks
        for task in list(self._background_tasks):
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        # 3. Cancel pending writes
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()

    async def _delete_local(
        self,
        pending: PendingWrite,
        key: bytes,
    ) -> ValueVersion | None:
        try:
            version = await self._storage.delete_local(key)
            pending.ack_success()
            return version
        except Exception as ex:
            self._logger.error(f"Unable to delete locally '{key}': {ex}")
            pending.ack_failure()
            return None

    async def _event_loop(self) -> None:
        async for event in self._subscription:
            if event is None:
                break
            self.on_event(event)

    async def _put_local(
        self,
        pending: PendingWrite,
        key: bytes,
        value: bytes
    ) -> ValueVersion | None:
        try:
            version = await self._storage.put_local(key, value)
            pending.ack_success()
            return version
        except Exception as ex:
            self._logger.error(f"Unable to put locally '{key}': {ex}")
            pending.ack_failure()

    def _track_task(self, task: asyncio.Task) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _write_ack(
        self,
        pending: PendingWrite,
        request_id: str,
        timeout: float
    ) -> bool:
        try:
            return await asyncio.wait_for(pending.future, timeout=timeout)
        except asyncio.TimeoutError:
            return False
        finally:
            self._pending.pop(request_id, None)

    async def _read_ack(
        self,
        pending: PendingRead,
        request_id: str,
        timeout: float
    ) -> ValueVersion | None:
        try:
            return await asyncio.wait_for(pending.future, timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self._pending.pop(request_id, None)
