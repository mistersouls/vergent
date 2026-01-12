import asyncio
import logging

from vergent.core.model.event import Event
from vergent.core.model.membership import Membership, MembershipDiff, MembershipChange
from vergent.core.bucket import BucketTable
from vergent.core.sub import Subscription


class GossipBucketSync:
    def __init__(
        self,
        bucket_table: BucketTable,
        publisher: Subscription[Event | None],
        loop: asyncio.AbstractEventLoop,
        max_parallel_fetch: int = 4,
    ) -> None:
        self._logger = logging.getLogger("vergent.core.gossip")

        self._bucket_table = bucket_table
        self._publisher = publisher
        self._loop = loop

        self._queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._pending: set[tuple[str, str]] = set()

        self._workers: list[asyncio.Task] = []
        self._max_parallel = max_parallel_fetch

        self._stop_event = asyncio.Event()

    def start(self) -> None:
        for _ in range(self._max_parallel):
            task = self._loop.create_task(self._sync_worker())
            self._workers.append(task)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._workers:
            task.cancel()

        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)

    async def handle_gossip(self, remote_checksums: dict[str, int], source: str) -> None:
        missing = self._bucket_table.compute_missing_buckets(remote_checksums)
        if not missing:
            return

        self._logger.debug(f"Missing buckets from {source}: {missing}")

        for bucket_id in missing:
            key = (source, bucket_id)
            if key not in self._pending:
                self._pending.add(key)
                await self._queue.put(key)

    def sync_memberships(self, bucket_id: str, source: str, memberships: list[Membership]) -> MembershipDiff:
        bucket = self._bucket_table.buckets[bucket_id]
        diff = MembershipDiff.from_empty(bucket_id)

        before = bucket.memberships.copy()
        self._bucket_table.merge_bucket(bucket_id, memberships)
        after = bucket.memberships.copy()

        before_ids = set(before.keys())
        after_ids = set(after.keys())

        # Added
        for node_id in after_ids - before_ids:
            diff.added.append(after[node_id])

        # Removed
        for node_id in before_ids - after_ids:
            diff.removed.append(before[node_id])

        # Updated
        for node_id in before_ids & after_ids:
            m_before = before[node_id]
            m_after = after[node_id]
            if m_before.size != m_after.size:
                diff.updated.append(MembershipChange(before=m_before, after=m_after))

        self._logger.debug(f"Merged bucket {bucket_id} from {source}")

        return diff

    async def _sync_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                source, bucket_id = await self._queue.get()

                try:
                    await self._sync_bucket_request(source, bucket_id)
                finally:
                    self._pending.discard((source, bucket_id))
                    self._queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.exception(f"Error in sync worker: {e}")

    async def _sync_bucket_request(self, source: str, bucket_id: str) -> None:
        event = Event(
            type="_sync/memberships",
            payload={
                "kind": "membership",
                "bucket_id": bucket_id,
                "target": source
            }
        )

        try:
            self._publisher.publish(event)
        except Exception as e:
            self._logger.error(f"Failed to publish sync request for bucket {bucket_id}: {e}")
