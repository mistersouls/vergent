import asyncio

from vergent.core.config import Config
from vergent.core.model.partition import Partitioner
from vergent.core.model.vnode import VNode
from vergent.core.storage.partitionned import PartitionedStorage
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.placement import PlacementStrategy
from vergent.core.ring import Ring
from vergent.core.types_ import StorageFactory
from vergent.core.utils.log import setup_logging


class VergentCore:
    def __init__(
        self,
        config: Config,
        storage_factory: StorageFactory,
        log_level: str,
    ) -> None:
        setup_logging(log_level)

        self.config = config
        self.ring = Ring(VNode.generate_vnodes(config.node_id, config.node_size))
        self.partitioner = Partitioner(config.partition_shift)
        self.placement = PlacementStrategy(self.ring, self.partitioner)
        self.storage = self._get_storage(storage_factory)
        self.loop = asyncio.get_event_loop()

    @staticmethod
    def _get_event_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

    def _get_storage(self, factory: StorageFactory) -> VersionedStorage:
        placement = PlacementStrategy(self.ring, self.partitioner, self.config.replication_factor)
        partitioned = PartitionedStorage(factory, placement)
        storage = VersionedStorage(partitioned, self.config.node_id)
        return storage
