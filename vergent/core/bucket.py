import hashlib
import zlib
import random

from vergent.core.gossip.frame import Serializer
from vergent.core.model.membership import Membership


class Bucket:
    def __init__(self, bucket_id: str) -> None:
        self.bucket_id = bucket_id
        self.memberships: dict[str, Membership] = {}
        self.checksum: int = 0
        self.dirty: bool = True

    def add_or_update(self, membership: Membership) -> None:
        self.memberships[membership.node_id] = membership
        self.dirty = True

    def recompute_checksum(self) -> None:
        items = sorted(self.memberships.values(), key=lambda m: m.node_id)

        crc = 0
        for membership in items:
            crc = zlib.crc32(Serializer.serialize(membership), crc)

        self.checksum = crc
        self.dirty = False

    def get_checksum(self) -> int:
        if self.dirty:
            self.recompute_checksum()
        return self.checksum

    def serialize_memberships(self) -> dict[str, dict]:
        return {
            node_id: membership.to_dict()
            for node_id, membership in self.memberships.items()
        }


class BucketTable:
    def __init__(self, total_buckets: int) -> None:
        self._total_buckets = total_buckets
        self.buckets: dict[str, Bucket] = {
            str(i): Bucket(str(i)) for i in range(total_buckets)
        }
        self._non_empty_buckets: set[str] = set()
        self._checksums_cache: dict[str, int] | None = None
        self._dirty_global: bool = True
        self._latest_epoch: int = 0

    @property
    def latest_epoch(self) -> int:
        return self._latest_epoch

    def _mark_dirty(self) -> None:
        self._dirty_global = True

    def bucket_for(self, peer_id: str) -> str:
        h = int(hashlib.md5(peer_id.encode()).hexdigest(), 16)
        return str(h % self._total_buckets)

    def add_or_update(self, membership: Membership) -> None:
        bucket_id = self.bucket_for(membership.node_id)
        bucket = self.buckets[bucket_id]

        bucket.add_or_update(membership)
        self._non_empty_buckets.add(bucket_id)
        if membership.epoch > self._latest_epoch:
            self._latest_epoch = membership.epoch

        self._mark_dirty()

    def remove(self, node_id: str) -> None:
        bucket_id = self.bucket_for(node_id)
        bucket = self.buckets[bucket_id]

        if node_id in bucket.memberships:
            del bucket.memberships[node_id]
            bucket.dirty = True

            if not bucket.memberships:
                self._non_empty_buckets.discard(bucket_id)

            self._mark_dirty()

    def get_checksums(self) -> dict[str, int]:
        if not self._dirty_global and self._checksums_cache is not None:
            return self._checksums_cache

        checksums = {}
        for i in range(self._total_buckets):
            idx = str(i)
            if idx in self._non_empty_buckets:
                checksums[idx] = self.buckets[idx].get_checksum()

        self._checksums_cache = checksums
        self._dirty_global = False
        return checksums

    def get_bucket_memberships(self, bucket_id: str) -> list[bytes]:
        return self.buckets[bucket_id].serialize_memberships()

    def merge_bucket(self, bucket_id: str, memberships: list[Membership]) -> None:
        bucket = self.buckets[bucket_id]
        changed = False

        if not memberships:
            if bucket.memberships:
                bucket.memberships.clear()
                bucket.dirty = True
                self._non_empty_buckets.discard(bucket_id)
                self._mark_dirty()
            return

        for m in memberships:
            local = bucket.memberships.get(m.node_id)
            if local is None or m.epoch > local.epoch:
                bucket.add_or_update(m)
                changed = True

        remote_ids = {m.node_id for m in memberships}
        local_ids = set(bucket.memberships.keys())

        for node_id in local_ids - remote_ids:
            del bucket.memberships[node_id]
            changed = True

        if changed:
            bucket.dirty = True
            self._non_empty_buckets.add(bucket_id)
            self._mark_dirty()

    def peek_random_member(self) -> Membership | None:
        if not self._non_empty_buckets:
            return None

        bucket_id = random.choice(list(self._non_empty_buckets))
        bucket = self.buckets[bucket_id]

        return random.choice(list(bucket.memberships.values()))
