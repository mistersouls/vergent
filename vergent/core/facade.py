import asyncio

from vergent.core.bucket import BucketTable
from vergent.core.config import ApiConfig, PeerConfig, ReplicationConfig
from vergent.core.coordinator import Coordinator
from vergent.core.model.event import Event
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ports.node import NodeMetaStore
from vergent.core.replication import PartitionTransfer
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription


class VergentCore:
    def __init__(
        self,
        api_config: ApiConfig,
        peer_config: PeerConfig,
        replication_config: ReplicationConfig,
        peer_state: PeerState,
        meta_store: NodeMetaStore,
        incoming: Subscription[Event | None],
        view: BucketTable,
        partitioner: Partitioner,
        storage: VersionedStorage,
        coordinator: Coordinator,
        pts: PartitionTransfer,
        peer_clients: PeerConnectionPool,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.api_config = api_config
        self.peer_config = peer_config
        self.replication_config = replication_config
        self.peer_state = peer_state
        self.meta_store = meta_store
        self.incoming = incoming
        self.view = view
        self.partitioner = partitioner
        self.storage = storage
        self.coordinator = coordinator
        self.pts = pts
        self.connection_pools = peer_clients
        self.loop = loop
