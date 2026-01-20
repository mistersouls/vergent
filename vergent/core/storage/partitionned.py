from typing import AsyncIterator

from vergent.core.model.partition import Partitioner
from vergent.core.types_ import Storage, StorageFactory


class PartitionedStorage(Storage):
    """
    A storage pool that routes operations to the correct backend
    based on the partitioning strategy.
    """

    def __init__(
        self,
        storage_factory: StorageFactory,
        partitioner: Partitioner,
    ) -> None:
        """
        backends: mapping partition_id -> Storage backend
        placement: object with route(key: bytes) -> (partition, vnode)
        """
        self._backends: dict[str, Storage] = {}
        self._storage_factory = storage_factory
        self._partitioner = partitioner

    def _select_backend(self, namespace: bytes) -> Storage:
        env_id = self._env_id_for(namespace)
        backend = self._backends.get(env_id)
        if backend is None:
            backend = self._storage_factory.create(env_id)
            self._backends[env_id] = backend
        return backend

    def _env_id_for(self, namespace: bytes) -> str:
        pid = int(namespace.decode("ascii"), 16)
        env_index = pid // self._storage_factory.get_max_namespaces()
        return str(env_index)

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        backend = self._select_backend(namespace)
        return await backend.get(namespace, key)

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        backend = self._select_backend(namespace)
        await backend.put(namespace, key, value)

    async def put_many(self, namespace: bytes, items: list[tuple[bytes, bytes]]) -> None:
        if not items:
            return

        backend = self._select_backend(namespace)
        await backend.put_many(namespace, items)

    async def delete(self, namespace: bytes, key: bytes) -> None:
        backend = self._select_backend(namespace)
        await backend.delete(namespace, key)

    async def iter(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        # _select_backend is not call here to optimize rebalancing.
        # This avoids creating unnecessary LMDB environments
        env_id = self._env_id_for(namespace)
        if backend := self._backends.get(env_id):
            async for key, value in backend.iter(
                namespace=namespace,
                limit=limit,
                batch_size=batch_size
            ):
                yield key, value
