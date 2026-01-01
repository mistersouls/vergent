import asyncio

from vergent.core.config import Config
from vergent.core.coordinator import Coordinator
from vergent.core.p2p.client import PeerClientPool
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.placement import PlacementStrategy


class VergentCore:
    def __init__(
        self,
        config: Config,
        placement: PlacementStrategy,
        storage: VersionedStorage,
        coordinator: Coordinator,
        peer_clients: PeerClientPool,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.config = config
        self.placement = placement
        self.storage = storage
        self.coordinator = coordinator
        self.peer_clients = peer_clients
        self.loop = loop
