import asyncio

from vergent.core.config import Config
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.placement import PlacementStrategy


class VergentCore:
    def __init__(
        self,
        config: Config,
        placement: PlacementStrategy,
        storage: VersionedStorage,
    ) -> None:
        self.config = config
        self.placement = placement
        self.storage = storage
        self.loop = self._get_event_loop()

    @staticmethod
    def _get_event_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
