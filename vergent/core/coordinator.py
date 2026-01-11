import asyncio
import logging

from vergent.core.exception import InternalError
from vergent.core.model.event import Event
from vergent.core.model.partition import Partitioner
from vergent.core.model.request import PutRequest
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.p2p.conflict import ValueVersion
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription


class Pending:
    def __init__(self, quorum: int, loop: asyncio.AbstractEventLoop):
        self.quorum = quorum
        self.responses: list[ValueVersion] = []
        self.failed = 0
        self.future: asyncio.Future[list[ValueVersion]] = loop.create_future()

    def ack_success(self, version: ValueVersion) -> None:
        if self.future.done():
            return

        self.responses.append(version)

        # When we reach quorum, complete the future with all collected versions.
        if len(self.responses) >= self.quorum:
            self.future.set_result(self.responses)

    def ack_failure(self) -> None:
        if self.future.done():
            return
        self.failed += 1
        # You might later decide to fail early if too many failures occur.


class Coordinator:
    def __init__(
        self,
        state: PeerState,
        peers: PeerConnectionPool,
        partitioner: Partitioner,
        subscription: Subscription[Event | None],
        storage: VersionedStorage,
        replication_factor: int,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self._state = state
        self._peers = peers
        self._partitioner = partitioner
        self._subscription = subscription
        self._storage = storage
        self._replication_factor = replication_factor
        self._loop = loop

        self._logger = logging.getLogger("vergent.core.coordinator")

        # request_id -> Pending
        self._pending: dict[str, Pending] = {}
        self._event_task = self._loop.create_task(self._event_loop())
        self._background_tasks: set[asyncio.Task] = set()

    @property
    def local_node_id(self) -> str:
        return self._state.membership.node_id

    def on_event(self, event: Event) -> None:
        payload = event.payload
        request_id = payload.get("request_id")

        if not request_id or event.type not in ("ok", "ko"):
            return

        pending = self._pending.get(request_id)
        if not pending:
            # Late response or already cleaned up.
            return

        if event.type == "ok":
            version_payload = payload.get("version")
            if version_payload is None:
                self._logger.warning(
                    "Received ok without version for request_id=%s", request_id
                )
                pending.ack_failure()
                return

            try:
                version = ValueVersion.from_dict(version_payload)
            except Exception as ex:
                self._logger.error(
                    "Failed to decode version for request_id=%s: %s",
                    request_id,
                    ex,
                    exc_info=ex,
                )
                pending.ack_failure()
                return

            pending.ack_success(version)
        else:
            pending.ack_failure()

    async def put(self, request: PutRequest) -> Event:
        """
        Handle a client PUT request.

        The local node either coordinates the write (if it is in charge
        of this key) or forwards the request to the appropriate primary
        coordinator.
        """
        primary = self.find_key_owner(request.key)

        if self.local_node_id != primary.node_id:
            return await self.forward_put(request, primary)

        return await self.coordinate_put(request, primary)

    async def coordinate_put(self, request: PutRequest, primary: VNode) -> Event:
        """
        Coordinate a PUT for which this node is the primary in the preference list.
        """
        ring = self._state.ring
        replicas = ring.preference_list(primary, self._replication_factor)

        request_id = request.request_id
        timeout = request.timeout

        pending = Pending(request.quorum_write, self._loop)
        self._pending[request_id] = pending

        try:
            # 1. Perform local write and count it toward quorum.
            local_version = await self._put_local(pending, request)

            # 2. Replicate to other replicas.
            replicate_event = Event(
                type="replicate",
                payload={
                    "key": request.key,
                    "version": local_version.to_dict(),
                    "request_id": request_id,
                },
            )

            for replica in replicas:
                if replica.node_id == self.local_node_id:
                    continue
                transport = self._peers.get(replica.node_id)
                # We do not await here; we track it as a background task.
                self._track_task(self._loop.create_task(transport.send(replicate_event)))

            # 3. Wait for W responses (including local).
            versions = await self._write_ack(pending, request_id, timeout)
            if versions:
                # For now, just return one version (e.g. the local or first).
                version = versions[0]
                payload = {"version": version.to_dict(), "request_id": request_id}
                return Event(type="ok", payload=payload)

            raise InternalError("Expected at least 1 version but got 0.")
        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {timeout}"
            self._logger.warning(
                "PUT coordination timed out for request_id=%s: %s",
                request_id,
                message,
            )
            return Event(type="ko", payload={"message": message, "request_id": request_id})
        except InternalError as ex:
            self._logger.error(
                "Internal error during PUT coordination for request_id=%s: %s",
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(type="ko", payload={"message": str(ex), "request_id": request_id})

        # Hinted hand-off can be implemented later.

    async def forward_put(self, request: PutRequest, primary: VNode) -> Event:
        """
        Forward the PUT to the primary coordinator and wait for its result.

        From the forwarding node perspective, we only need one response:
        the primary's result (which itself is based on its own quorum).
        """
        request_id = request.request_id

        payload = {
            "kind": "put",
            "key": request.key,
            "value": request.value,
            "W": request.quorum_write,
            "request_id": request_id,
            "timeout": request.timeout,
        }
        event = Event(type="forward", payload=payload)
        transport = self._peers.get(primary.node_id)

        # From this node's perspective, quorum is 1: the primary's response.
        pending = Pending(1, self._loop)
        self._pending[request_id] = pending

        await transport.send(event)

        try:
            versions = await self._write_ack(pending, request_id, request.timeout)
            if versions:
                version = versions[0]
                payload = {"version": version.to_dict(), "request_id": request_id}
                return Event(type="ok", payload=payload)
            raise InternalError("Expected 1 version but got 0.")
        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {request.timeout}"
            self._logger.warning(
                "PUT forward timed out for request_id=%s: %s",
                request_id,
                message,
            )
            return Event(type="ko", payload={"message": message, "request_id": request_id})
        except InternalError as ex:
            self._logger.error(
                "Internal error during PUT forward for request_id=%s: %s",
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(type="ko", payload={"message": str(ex), "request_id": request_id})

    def find_key_owner(self, key: bytes) -> VNode:
        ring = self._state.ring
        placement = self._partitioner.find_placement_by_key(key, ring)
        return placement.vnode

    async def close(self) -> None:
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

        # 3. Cancel pending operations
        for pending in self._pending.values():
            if not pending.future.done():
                pending.future.cancel()
        self._pending.clear()

    async def _event_loop(self) -> None:
        async for event in self._subscription:
            if event is None:
                break
            self.on_event(event)

    async def _put_local(self, pending: Pending, request: PutRequest) -> ValueVersion:
        """
        Perform the local PUT and count it toward the quorum.
        """
        key = request.key
        value = request.value

        try:
            version = await self._storage.put_local(key, value)
            pending.ack_success(version)
            return version
        except Exception as ex:
            self._logger.error(
                "Unable to put locally '%s': %s", key, ex, exc_info=ex
            )
            pending.ack_failure()
            raise InternalError(f"Internal error while putting locally '{key}'") from ex

    def _track_task(self, task: asyncio.Task) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _write_ack(
        self,
        pending: Pending,
        request_id: str,
        timeout: float,
    ) -> list[ValueVersion]:
        """
        Wait for quorum acknowledgments or timeout.

        Cleanup of the pending entry is centralized here to avoid races
        and double-removal.
        """
        try:
            return await asyncio.wait_for(pending.future, timeout=timeout)
        finally:
            # Ensure we always clean up the pending map for this request.
            self._pending.pop(request_id, None)
