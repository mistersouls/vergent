from typing import AsyncIterator, Mapping, Any

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

    async def get(self, key: bytes) -> bytes | None:
        version = await self.get_version(key)
        if version is None or version.is_tombstone:
            return None
        return version.value

    async def put(self, key: bytes, value: bytes) -> None:
        await self.put_local(key, value)

    async def delete(self, key: bytes) -> None:
        await self.delete_local(key)

    async def iter(self, limit: int = -1, batch_size: int = 1024) -> AsyncIterator[tuple[str, bytes]]:
        async for key, version in self.iter_versions(limit=limit, batch_size=batch_size):
            if not version.is_tombstone:
                yield key, version.value

    async def compute_digest(self, batch_size: int = 1024) -> Mapping[str, Mapping[str, Any]]:
        digest:dict[str, dict[str, Any]] = {}

        async for key, version in self.iter_versions(batch_size=batch_size):
            digest[key] = version.hlc.to_dict()

        return digest

    async def get_version(self, key: bytes) -> ValueVersion | None:
        if raw := await self._backend.get(key):
            return ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
        return None

    async def put_local(self, key: bytes, value: bytes) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.from_bytes(value, ts, origin=self._node_id)
        await self._backend.put(key, msgpack.packb(version.to_dict(), use_bin_type=True))
        return version

    async def delete_local(self, key: bytes) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.tombstone(ts, origin=self._node_id)
        await self._backend.put(key, msgpack.packb(version.to_dict(), use_bin_type=True))
        return version

    async def iter_versions(self, limit: int = -1, batch_size: int = 1024) -> AsyncIterator[tuple[str, ValueVersion]]:
        async for key, raw in self._backend.iter(limit=limit, batch_size=batch_size):
            version = ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
            yield key, version

    async def apply_remote_version(self, key: bytes, remote: ValueVersion) -> ValueVersion:
        # Advance local HLC based on remote timestamp
        self._hlc = self._hlc.tick_on_receive(remote.hlc)

        local = await self.get_version(key)
        candidates = [v for v in (local, remote) if v is not None]
        winner = ConflictResolver.resolve(candidates)

        if winner != local:
            await self._backend.put(key, msgpack.packb(winner.to_dict(), use_bin_type=True))

        return winner
