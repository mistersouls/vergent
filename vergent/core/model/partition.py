from dataclasses import dataclass


class Partitioner:
    def __init__(self, partition_shift: int) -> None:
        self._partition_shift = partition_shift

    @property
    def total_partitions(self) -> int:
        """Q"""
        return 1 << self._partition_shift

    @property
    def step(self) -> int:
        return (1 << 128) >> self._partition_shift

    def pid_for_hash(self, h: int) -> int:
        """
        Compute the partition id for a 128-bit hash.
        Equivalent to: pid = h // step
        Implemented as: pid = h >> (128 - partition_shift)
        """
        return h >> (128 - self._partition_shift)

    def start_for_pid(self, pid: int) -> int:
        return pid * self.step

    def end_for_pid(self, pid: int) -> int:
        return (pid + 1) * self.step

    def segment_for_pid(self, pid: int) -> tuple[int, int]:
        return self.start_for_pid(pid), self.end_for_pid(pid)

    def partition_for_hash(self, h: int) -> LogicalPartition:
        """
        Construct and return the LogicalPartition that contains the hash h.
        """
        pid = self.pid_for_hash(h)
        start, end = self.segment_for_pid(pid)
        return LogicalPartition(pid, start, end)


@dataclass(frozen=True, slots=True)
class LogicalPartition:
    """
    LogicalPartition represents a fixed segment of the 128-bit hash space.

    The hash space is divided into Q equal-sized logical partitions. These
    partitions are static, globally defined, and independent of cluster topology.
    Each partition covers a half-open interval (start, end] on the ring.

    Logical partitions do not store data and do not know anything about nodes,
    vnodes, or replication. They are purely a mathematical subdivision of the
    hash space. Higher-level components map partitions to VNodes and physical
    nodes.
    """
    pid: int
    start: int
    end: int

    @property
    def strong_id(self) -> str:
        return f"{self.pid}-{self.start:0x32}:{self.end}"

    def contains(self, h: int) -> bool:
        """Reports whether the hash h belongs to this partition."""
        return self.start < h <= self.end
