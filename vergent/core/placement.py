from vergent.core.model.partition import Partitioner, LogicalPartition
from vergent.core.model.vnode import VNode
from vergent.core.ring import Ring
from vergent.core.space import HashSpace


class PlacementStrategy:
    """
    Combines the Partitioner and the Ring to provide:
        - routing (partition → vnode → node_id)
        - replica selection
        - rebalance planning
    """

    def __init__(
        self,
        ring: Ring,
        partitioner: Partitioner,
        replication_factor: int = 3
    ) -> None:
        self._ring = ring
        self._partitioner = partitioner
        self._replication_factor = replication_factor

    def find_partition_by_key(self, key: bytes) -> LogicalPartition:
        token = HashSpace.hash(key)
        partition = self._partitioner.partition_for_hash(token)
        return partition

    def find_vnode_by_partition(self, partition: LogicalPartition) -> VNode:
        vnode = self._ring.find_successor(partition.end)
        return vnode
