import asyncio
import logging

from vergent.core.exception import InternalError
from vergent.core.model.event import Event
from vergent.core.model.partition import Partitioner
from vergent.core.model.request import PutRequest, GetRequest, DeleteRequest
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.p2p.conflict import ValueVersion, ConflictResolver
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription


class Pending:
    def __init__(self, quorum: int, loop: asyncio.AbstractEventLoop):
        self.quorum = quorum
        # Each response can be a ValueVersion or None (meaning "key not found" on that replica).
        self.responses: list[ValueVersion | None] = []
        self.failed = 0
        self.future: asyncio.Future[list[ValueVersion | None]] = loop.create_future()

    def ack_success(self, version: ValueVersion | None) -> None:
        if self.future.done():
            return

        self.responses.append(version)

        # When we reach quorum, complete the future with all collected responses.
        if len(self.responses) >= self.quorum:
            self.future.set_result(self.responses)

    def ack_failure(self) -> None:
        if self.future.done():
            return
        self.failed += 1
        # We might later decide to fail early if too many failures occur.


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

        self._pending: dict[str, Pending] = {}
        self._event_task = self._loop.create_task(self._event_loop())
        self._background_tasks: set[asyncio.Task] = set()

    @property
    def local_node_id(self) -> str:
        return self._state.membership.node_id

    async def put(self, request: PutRequest) -> Event:
        primary = self.find_key_owner(request.key)

        if self.local_node_id != primary.node_id:
            return await self.forward_put(request, primary)

        return await self.coordinate_put(request, primary)

    async def get(self, request: GetRequest) -> Event:
        primary = self.find_key_owner(request.key)

        if self.local_node_id != primary.node_id:
            return await self.forward_get(request, primary)

        return await self.coordinate_get(request, primary)

    async def delete(self, request: DeleteRequest) -> Event:
        primary = self.find_key_owner(request.key)

        if self.local_node_id != primary.node_id:
            return await self.forward_delete(request, primary)

        return await self.coordinate_delete(request, primary)

    def on_event(self, event: Event) -> None:
        request_id = event.payload.get("request_id")
        if not request_id:
            return

        pending = self._pending.get(request_id)
        if not pending:
            return

        handler = {
            "ok": self._handle_ok,
            "ko": self._handle_ko,
            "sync/fetch": self._handle_sync_fetch,
        }.get(event.type)

        if handler is None:
            # Unknown or irrelevant event type
            return

        handler(event, pending)

    async def coordinate_put(self, request: PutRequest, primary: VNode) -> Event:
        return await self._coordinate_write(request, primary)

    async def coordinate_delete(self, request: DeleteRequest, primary: VNode) -> Event:
        return await self._coordinate_write(request, primary)

    async def coordinate_get(self, request: GetRequest, primary: VNode) -> Event:
        """
        Coordinate a GET for which this node is the primary.
        """
        ring = self._state.ring
        replicas = ring.preference_list(primary, self._replication_factor)

        request_id = request.request_id
        timeout = request.timeout

        pending = Pending(request.quorum_read, self._loop)
        self._pending[request_id] = pending

        try:
            # 1. Local read (None means "no value" locally).
            local_version = await self._storage.get_version(request.key)
            pending.ack_success(local_version)

            # 2. Ask replicas via sync/fetch.
            get_event = Event(
                type="sync",
                payload={
                    "kind": "fetch",
                    "keys": [request.key],
                    "request_id": request_id,
                },
            )

            for replica in replicas:
                if replica.node_id == self.local_node_id:
                    continue
                transport = self._peers.get(replica.node_id)
                self._track_task(self._loop.create_task(transport.send(get_event)))

            # 3. Wait for quorum and resolve to a single version (or None).
            best = await self._resolve_pending(pending, request_id, timeout)

            # No replica has the key or the winning version is a tombstone.
            if best is None or best.is_tombstone:
                return Event(
                    type="ok",
                    payload={"version": None, "request_id": request_id},
                )

            return Event(
                type="ok",
                payload={"version": best.to_dict(), "request_id": request_id},
            )

        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {timeout}"
            self._logger.warning(
                "GET coordination timed out for request_id=%s: %s",
                request_id,
                message,
            )
            return Event(
                type="ko",
                payload={"message": message, "request_id": request_id},
            )

        except InternalError as ex:
            self._logger.error(
                "Internal error during GET coordination for request_id=%s: %s",
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(
                type="ko",
                payload={"message": str(ex), "request_id": request_id},
            )

    async def forward_put(self, request: PutRequest, primary: VNode) -> Event:
        return await self._forward_write(request, primary)

    async def forward_delete(self, request: DeleteRequest, primary: VNode) -> Event:
        return await self._forward_write(request, primary)

    async def forward_get(self, request: GetRequest, primary: VNode) -> Event:
        """
        Forward the GET to the primary coordinator and wait for its result.

        From this node's perspective, quorum is 1: the primary's response,
        which itself is based on its own read quorum.
        """
        request_id = request.request_id

        payload = {
            "kind": "get",
            "key": request.key,
            "R": request.quorum_read,
            "request_id": request_id,
            "timeout": request.timeout,
        }
        event = Event(type="forward", payload=payload)
        transport = self._peers.get(primary.node_id)

        pending = Pending(1, self._loop)
        self._pending[request_id] = pending

        await transport.send(event)

        try:
            best = await self._resolve_pending(pending, request_id, request.timeout)

            # The primary is allowed to answer "no value".
            if best is None or best.is_tombstone:
                return Event(
                    type="ok",
                    payload={"version": None, "request_id": request_id},
                )

            return Event(
                type="ok",
                payload={"version": best.to_dict(), "request_id": request_id},
            )

        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {request.timeout}"
            self._logger.warning(
                "GET forward timed out for request_id=%s: %s",
                request_id,
                message,
            )
            return Event(
                type="ko",
                payload={"message": message, "request_id": request_id},
            )
        except InternalError as ex:
            self._logger.error(
                "Internal error during GET forward for request_id=%s: %s",
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(
                type="ko",
                payload={"message": str(ex), "request_id": request_id},
            )

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

    def _handle_ok(self, event: Event, pending: Pending) -> None:
        payload = event.payload

        if "version" not in payload:
            self._logger.warning(
                "Received ok without version field for request_id=%s",
                payload.get("request_id"),
            )
            pending.ack_failure()
            return

        version_payload = payload["version"]

        if version_payload is None:
            pending.ack_success(None)
            return

        try:
            version = ValueVersion.from_dict(version_payload)
        except Exception as ex:
            self._logger.error(
                "Failed to decode version for request_id=%s: %s",
                payload.get("request_id"),
                ex,
                exc_info=ex,
            )
            pending.ack_failure()
            return

        pending.ack_success(version)

    @staticmethod
    def _handle_ko(_: Event, pending: Pending) -> None:
        pending.ack_failure()

    def _handle_sync_fetch(self, event: Event, pending: Pending) -> None:
        payload = event.payload
        versions_map = payload.get("versions")

        if versions_map is None or not isinstance(versions_map, dict):
            self._logger.warning(
                "Received sync/fetch without valid 'versions' for request_id=%s",
                payload.get("request_id"),
            )
            pending.ack_failure()
            return

        if not versions_map:
            pending.ack_success(None)
            return

        # Only one key was requested in this path, so take the first entry.
        _, version_payload = next(iter(versions_map.items()))

        if version_payload is None:
            pending.ack_success(None)
            return

        try:
            version = ValueVersion.from_dict(version_payload)
        except Exception as ex:
            self._logger.error(
                "Failed to decode version from sync/fetch for request_id=%s: %s",
                payload.get("request_id"),
                ex,
                exc_info=ex,
            )
            pending.ack_failure()
            return

        pending.ack_success(version)

    async def _coordinate_write(
        self,
        request: PutRequest | DeleteRequest,
        primary: VNode,
    ) -> Event:
        """
        Generic coordinator for write operations (PUT and DELETE).
        """
        ring = self._state.ring
        replicas = ring.preference_list(primary, self._replication_factor)

        request_id = request.request_id
        timeout = request.timeout

        pending = Pending(request.quorum_write, self._loop)
        self._pending[request_id] = pending

        try:
            # 1. Perform local write (PUT or DELETE).
            if isinstance(request, PutRequest):
                local_version = await self._put_local(pending, request)
            else:
                local_version = await self._delete_local(pending, request)

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

            # 3. Wait for W responses (including local), then resolve to a single version.
            best = await self._resolve_pending(pending, request_id, timeout)

            if best is None:
                # For writes, getting no version at all despite quorum would be a logic error.
                raise InternalError("Expected at least one version for write, got None.")

            payload = {"version": best.to_dict(), "request_id": request_id}
            return Event(type="ok", payload=payload)

        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {timeout}"
            self._logger.warning(
                "Write coordination timed out for request_id=%s: %s",
                request_id,
                message,
            )
            return Event(
                type="ko",
                payload={"message": message, "request_id": request_id},
            )
        except InternalError as ex:
            self._logger.error(
                "Internal error during write coordination for request_id=%s: %s",
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(
                type="ko",
                payload={"message": str(ex), "request_id": request_id},
            )

    async def _forward_write(
        self,
        request: PutRequest | DeleteRequest,
        primary: VNode
    ) -> Event:
        """
        Generic forwarding for write operations (PUT and DELETE).
        """
        request_id = request.request_id
        kind = "put" if isinstance(request, PutRequest) else "delete"

        payload = {
            "kind": kind,
            "key": request.key,
            "W": request.quorum_write,
            "request_id": request_id,
            "timeout": request.timeout,
        }

        # PUT additionally needs the value
        if kind == "put":
            payload["value"] = request.value

        event = Event(type="forward", payload=payload)
        transport = self._peers.get(primary.node_id)

        pending = Pending(1, self._loop)
        self._pending[request_id] = pending

        await transport.send(event)

        try:
            best = await self._resolve_pending(pending, request_id, request.timeout)

            if best is None:
                raise InternalError(f"Primary did not return a version for {kind.upper()}.")

            return Event(
                type="ok",
                payload={"version": best.to_dict(), "request_id": request_id},
            )

        except asyncio.TimeoutError:
            message = f"Timeout exceeded: {request.timeout}"
            self._logger.warning(
                "%s forward timed out for request_id=%s: %s",
                kind.upper(),
                request_id,
                message,
            )
            return Event(type="ko", payload={"message": message, "request_id": request_id})

        except InternalError as ex:
            self._logger.error(
                "Internal error during %s forward for request_id=%s: %s",
                kind.upper(),
                request_id,
                ex,
                exc_info=ex,
            )
            return Event(type="ko", payload={"message": str(ex), "request_id": request_id})

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

    async def _delete_local(self, pending: Pending, request: DeleteRequest) -> ValueVersion:
        """
        Perform the local DELETE and count it toward the quorum.
        """
        key = request.key

        try:
            version = await self._storage.delete_local(key)
            pending.ack_success(version)
            return version
        except Exception as ex:
            self._logger.error(
                "Unable to delete locally '%s': %s", key, ex, exc_info=ex
            )
            pending.ack_failure()
            raise InternalError(f"Internal error while deleting locally '{key}'") from ex

    def _track_task(self, task: asyncio.Task) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _resolve_pending(
        self,
        pending: Pending,
        request_id: str,
        timeout: float,
    ) -> ValueVersion | None:
        """
        Wait for quorum responses or timeout, then resolve to a single version.

        - Waits for Pending.future (which completes when quorum responses are collected)
        - Filters out None (meaning "no value" on that replica)
        - Applies ConflictResolver on remaining versions
        - Returns the winning ValueVersion or None if no replica has the key
        """
        try:
            responses = await asyncio.wait_for(pending.future, timeout=timeout)
        finally:
            # Always clean up the pending entry
            self._pending.pop(request_id, None)

        # Filter out None (meaning "key not found" on that replica).
        real_versions = [v for v in responses if v is not None]

        # No replica has the key.
        if not real_versions:
            return None

        # Resolve conflicts using LWW/HLC.
        best = ConflictResolver.resolve(real_versions)
        return best
