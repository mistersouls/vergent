import asyncio
from concurrent.futures import ThreadPoolExecutor

import lmdb


class LMDBStorage:
    def __init__(
        self,
        path: str,
        map_size: int = 1 << 30,
        max_workers: int = 1,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = False
    ) -> None:
        # Need only 1 thread for optimal lmdb
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._env = lmdb.open(
            path,
            map_size=map_size,
            max_dbs=1,
            lock=True,
            writemap=writemap,
            sync=sync,
            readahead=readahead,
        )

    async def get(self, key: str) -> bytes | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get,
            key.encode(),
        )

    async def put(self, key: str, value: bytes) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_put,
            key.encode(),
            value
        )

    async def delete(self, key: str) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_delete,
            key.encode(),
        )

    def _sync_get(self, key: bytes) -> bytes | None:
        with self._env.begin(write=False) as txn:
            return txn.get(key)

    def _sync_put(self, key: bytes, value: bytes) -> None:
        with self._env.begin(write=True) as txn:
            txn.put(key, value)

    def _sync_delete(self, key: bytes) -> None:
        with self._env.begin(write=True) as txn:
            txn.delete(key)
