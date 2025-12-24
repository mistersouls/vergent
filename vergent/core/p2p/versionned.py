import msgpack

from vergent.core.p2p.conflict import ValueVersion, ConflictResolver
from vergent.core.p2p.hlc import HLC
from vergent.core.types_ import Storage


class VersionedStorage:
    """
    A versioned key-value store built on top of a raw Storage backend.

    Responsibilities:
        - versioned PUT/DELETE using HLC
        - tombstone support
        - LWW conflict resolution
        - serialization/deserialization of ValueVersion
        - apply remote versions (replication / anti-entropy)
    """
    def __init__(self, backend: Storage, node_id: str) -> None:
        self._backend = backend
        self._node_id = node_id
        self._hlc = HLC.initial(node_id)

    async def get(self, key: str) -> bytes | None:
        version = await self.get_version(key)
        if version is None or version.is_tombstone:
            return None
        return version.value

    async def put(self, key: str, value: bytes) -> None:
        await self.put_local(key, value)

    async def delete(self, key: str) -> None:
        await self.delete_local(key)

    async def get_version(self, key: str) -> ValueVersion | None:
        if raw := await self._backend.get(key):
            return ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
        return None

    async def put_local(self, key: str, value: bytes) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.from_bytes(value, ts, origin=self._node_id)
        await self._backend.put(key, msgpack.packb(version.to_dict()))
        return version

    async def delete_local(self, key: str) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.tombstone(ts, origin=self._node_id)
        await self._backend.put(key, msgpack.packb(version.to_dict()))
        return version

    async def apply_remote_version(self, key: str, remote: ValueVersion) -> ValueVersion:
        # Advance local HLC based on remote timestamp
        self._hlc = self._hlc.tick_on_receive(remote.hlc)

        local = await self.get_version(key)
        candidates = [v for v in (local, remote) if v is not None]
        winner = ConflictResolver.resolve(candidates)

        if winner != local:
            await self._backend.put(key, msgpack.packb(winner.to_dict()))

        return winner
