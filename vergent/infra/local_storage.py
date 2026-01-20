import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import AsyncIterator

import lmdb

from vergent.core.types_ import Storage


class LMDBStorage:
    def __init__(
        self,
        path: str,
        map_size: int = 1 << 30,
        max_workers: int = 1,
        max_dbs: int = 256,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = False
    ) -> None:
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._env = lmdb.open(
            path,
            map_size=map_size,
            max_dbs=max_dbs,
            lock=True,
            writemap=writemap,
            sync=sync,
            readahead=readahead,
        )
        self._dbis: dict[bytes, object] = {}

    async def get(self, namespace: bytes, key: bytes) -> bytes | None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_get,
            namespace,
            key,
        )

    async def put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_put,
            namespace,
            key,
            value
        )

    async def put_many(self, namespace: bytes, items: list[tuple[bytes, bytes]]) -> None:
        """
        Insert many (key, value) pairs in a single LMDB write transaction.
        Much faster than calling put() repeatedly.
        """
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            self._executor,
            self._sync_put_many,
            namespace,
            items,
        )

    async def delete(self, namespace: bytes, key: bytes) -> None:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            self._sync_delete,
            namespace,
            key,
        )

    async def iter(
        self,
        namespace: bytes,
        limit: int = -1,
        batch_size: int = 1024
    ) -> AsyncIterator[tuple[bytes, bytes]]:
        """
        Asynchronously iterate over all key/value pairs in LMDB.

        - batch_size: number of entries to fetch per LMDB transaction
        - limit: max number of entries to return (-1 = no limit)

        This is scalable and LMDB‑safe:
        - LMDB scan happens entirely inside the threadpool
        - no transaction or cursor crosses thread boundaries
        - async yields happen outside LMDB
        """
        loop = asyncio.get_running_loop()
        start_key = None

        remaining = limit if limit != -1 else None

        while True:
            # Fetch one batch inside LMDB thread
            batch: list[tuple[bytes, bytes]] = await loop.run_in_executor(
                self._executor,
                self._sync_iter,
                namespace,
                start_key,
                batch_size
            )

            if not batch:
                break

            # Yield results asynchronously
            for key, value in batch:
                yield key, value

                if remaining is not None:
                    remaining -= 1
                    if remaining <= 0:
                        return

            start_key = batch[-1][0] + b"\x00"

    def _get_dbis(self, namespace: bytes) -> object:
        dbi = self._dbis.get(namespace)
        if dbi is None:
            dbi = self._env.open_db(namespace)
            self._dbis[namespace] = dbi
        return dbi

    def _sync_get(self, namespace: bytes, key: bytes) -> bytes | None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=False) as txn:
            return txn.get(key)

    def _sync_put(self, namespace: bytes, key: bytes, value: bytes) -> None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=True) as txn:
            txn.put(key, value)

    def _sync_delete(self, namespace: bytes, key: bytes) -> None:
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=True) as txn:
            txn.delete(key)

    def _sync_iter(self, namespace: bytes, start_key: bytes | None, limit: int) -> list[tuple[bytes, bytes]]:
        """
        Scan at most `limit` keys starting from `start_key`.

        Runs entirely inside the LMDB thread (safe).
        """
        items = []

        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=False) as txn:
            with txn.cursor() as cursor:
                if start_key is None:
                    has_key = cursor.first()
                else:
                    has_key = cursor.set_range(start_key)

                while has_key and len(items) < limit:
                    items.append((cursor.key(), cursor.value()))
                    has_key = cursor.next()

        return items

    def _sync_put_many(self, namespace: bytes, items: list[tuple[bytes, bytes]]) -> None:
        """
        Synchronous batch insert inside a single LMDB write transaction.
        Runs entirely inside the LMDB thread (safe).
        """
        dbi = self._get_dbis(namespace)
        with self._env.begin(db=dbi, write=True) as txn:
            for key, value in items:
                txn.put(key, value)


class LMDBStorageFactory:
    def __init__(
        self,
        path: Path,
        map_size: int = 1 << 30,
        max_dbs: int = 256,
        max_workers: int = 1,
        readahead: bool = True,
        writemap: bool = False,
        sync: bool = False
    ) -> None:
        self._path = path
        self._map_size = map_size
        self._max_dbs = max_dbs
        self._max_workers = max_workers
        self._readahead = readahead
        self._writemap = writemap
        self._sync = sync

    def create(self, sid: str) -> Storage:
        path = self._path / sid
        path.mkdir(exist_ok=True)
        return LMDBStorage(
            path=str(path),
            map_size=self._map_size,
            max_dbs=self._max_dbs,
            max_workers=self._max_workers,
            readahead=self._readahead,
            writemap=self._writemap,
            sync=self._sync
        )

    def get_max_namespaces(self) -> int:
        return self._max_dbs
