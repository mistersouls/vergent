from typing import AsyncIterator

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

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        version = await self.get_version(namespace, key)
        if version is None or version.is_tombstone:
            return None
        return version.value

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        await self.put_local(namespace, key, value)

    async def delete(self, namespace: bytes, key: bytes) -> None:
        await self.delete_local(namespace, key)

    async def iter(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        async for key, version in self.iter_versions(
            namespace=namespace,
            limit=limit,
            batch_size=batch_size
        ):
            if not version.is_tombstone:
                yield key, version.value

    async def get_version(self, namespace: bytes, key: bytes) -> ValueVersion | None:
        if raw := await self._backend.get(namespace, key):
            return ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
        return None

    async def put_local(self, namespace: bytes, key: bytes, value: bytes) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.from_bytes(value, ts, origin=self._node_id)
        await self._backend.put(namespace, key, msgpack.packb(version.to_dict(), use_bin_type=True))
        return version

    async def delete_local(self, namespace: bytes, key: bytes) -> ValueVersion:
        ts = self._hlc = self._hlc.tick_local()
        version = ValueVersion.tombstone(ts, origin=self._node_id)
        await self._backend.put(namespace, key, msgpack.packb(version.to_dict(), use_bin_type=True))
        return version

    async def iter_raw(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        async for key, raw in self._backend.iter(
            namespace=namespace,
            limit=limit,
            batch_size=batch_size
        ):
            yield key, raw

    async def iter_versions(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, ValueVersion]]:
        async for key, raw in self._backend.iter(
            namespace=namespace,
            limit=limit,
            batch_size=batch_size
        ):
            version = ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
            yield key, version

    async def apply(
        self,
        namespace: bytes,
        key: bytes,
        remote: ValueVersion
    ) -> ValueVersion:
        # Advance local HLC based on remote timestamp
        self._hlc = self._hlc.tick_on_receive(remote.hlc)

        local = await self.get_version(namespace, key)
        candidates = [v for v in (local, remote) if v is not None]
        winner = ConflictResolver.resolve(candidates)

        if winner != local:
            await self._backend.put(
                namespace, key, msgpack.packb(winner.to_dict(), use_bin_type=True)
            )

        return winner

    async def apply_many(self, namespace: bytes, items: list[tuple[bytes, bytes]]) -> None:
        to_write: list[tuple[bytes, bytes]] = []

        for key, raw in items:
            remote = ValueVersion.from_dict(msgpack.unpackb(raw, raw=False))
            self._hlc = self._hlc.tick_on_receive(remote.hlc)
            local = await self.get_version(namespace, key)
            candidates = [v for v in (local, remote) if v is not None]
            winner = ConflictResolver.resolve(candidates)
            if winner != local:
                value = msgpack.packb(winner.to_dict(), use_bin_type=True)
                to_write.append((key, value))

        if to_write:
            await self._backend.put_many(namespace, to_write)
