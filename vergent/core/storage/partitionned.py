from typing import AsyncIterator

from vergent.core.model.partition import Partitioner
from vergent.core.types_ import Storage, StorageFactory


class PartitionedStorage(Storage):
    """
    A storage pool that routes operations to the correct backend
    based on the partitioning strategy.
    """

    def __init__(self, storage_factory: StorageFactory, partitioner: Partitioner) -> None:
        """
        backends: mapping partition_id -> Storage backend
        placement: object with route(key: bytes) -> (partition, vnode)
        """
        self._backends: dict[str, Storage] = {}
        self._storage_factory = storage_factory
        self._partitioner = partitioner

    def _select_backend(self, key: bytes) -> Storage:
        partition = self._partitioner.find_partition_by_key(key)
        backend = self._backends.get(str(partition.pid))
        if backend is None:
            backend = self._storage_factory.create(str(partition.pid))
        return backend

    async def get(self, key: bytes) -> bytes | None:
        backend = self._select_backend(key)
        return await backend.get(key)

    async def put(self, key: bytes, value: bytes) -> None:
        backend = self._select_backend(key)
        await backend.put(key, value)

    async def delete(self, key: bytes) -> None:
        backend = self._select_backend(key)
        await backend.delete(key)

    async def iter(
        self,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[str, bytes]]:
        """
        Iterate over all partitions sequentially.
        """
        # fixme(souls): should be able to paginate
        remaining = limit

        for backend in self._backends.values():
            async for key, value in backend.iter(
                limit=remaining,
                batch_size=batch_size
            ):
                yield key, value
                if remaining > 0:
                    remaining -= 1
                    if remaining == 0:
                        return
