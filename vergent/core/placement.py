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

    def __init__(self, ring: Ring, partitioner: Partitioner, rf: int = 3):
        self._ring = ring
        self._partitioner = partitioner
        self._rf = rf

    def find_partition_by_key(self, key: bytes) -> LogicalPartition:
        token = HashSpace.hash(key)
        partition = self._partitioner.partition_for_hash(token)
        return partition

    def find_vnode_by_partition(self, partition: LogicalPartition) -> VNode:
        vnode = self._ring.find_successor(partition.end)
        return vnode

    def preference_list(self, partition: LogicalPartition) -> list[VNode]:
        """
        Return RF distinct vnodes responsible for this partition.
        """
        vnode = self._ring.find_successor(partition.end)
        replicas = [vnode]

        idx = self._ring.index_of(vnode)
        for _ in range(1, self._rf):
            idx = (idx + 1) % len(self._ring)
            replicas.append(self._ring[idx])

        return replicas

    # ------------------------------------------------------------
    # Rebalance planning
    # ------------------------------------------------------------

    def rebalance_plan(self, new_ring: Ring) -> list[dict]:
        """
        Compute a migration plan between the current ring and a new ring.
        Returns a list of migration operations:
            { partition, from_node, to_node }
        """
        plan = []

        for pid in range(self._partitioner.total_partitions):
            partition = self._partitioner.partition_for_pid(pid)

            old_owner = self._ring.find_successor(partition.end).node_id
            new_owner = new_ring.find_successor(partition.end).node_id

            if old_owner != new_owner:
                plan.append({
                    "partition": partition,
                    "from": old_owner,
                    "to": new_owner,
                })

        return plan
