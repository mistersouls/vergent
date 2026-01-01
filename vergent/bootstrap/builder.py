from pathlib import Path

from vergent.bootstrap.config.meta import VNodeMeta
from vergent.bootstrap.config.settings import VergentConfig
from vergent.core.app import App
from vergent.core.config import Config
from vergent.core.facade import VergentCore
from vergent.core.model.partition import Partitioner
from vergent.core.model.vnode import VNode
from vergent.core.placement import PlacementStrategy
from vergent.core.ring import Ring
from vergent.core.space import HashSpace
from vergent.core.storage.partitionned import PartitionedStorage
from vergent.core.storage.versionned import VersionedStorage
from vergent.infra.local_storage import LMDBStorageFactory


class CoreBuilder:
    """
    Constructs a fully initialized VergentCore instance.

    Responsibilities:
    - Load or generate vnode metadata
    - Build the ring
    - Build the partitioner
    - Build the placement strategy
    - Build the storage stack
    """

    def __init__(self, app: App, config: VergentConfig) -> None:
        self.config = config
        self.app = app

    def build(self) -> VergentCore:
        meta = self._load_or_generate_meta()
        vnodes = [VNode(token, meta.node_id) for token in meta.tokens]

        ring = Ring(vnodes)
        partitioner = Partitioner(self.config.placement.shift)

        placement = PlacementStrategy(
            ring=ring,
            partitioner=partitioner,
            replication_factor=self.config.placement.replication_factor,
        )

        storage = self._build_storage(placement)


        return VergentCore(
            config=self._build_config(),
            placement=placement,
            storage=storage,
        )

    def _meta_path(self) -> Path:
        return self.config.storage.data_dir / "meta" / "vnodes.meta"

    def _load_or_generate_meta(self) -> VNodeMeta:
        path = self._meta_path()

        if path.exists():
            meta = VNodeMeta.load(path)
            self._validate_meta(meta)
            return meta

        # Generate new vnode metadata
        tokens = HashSpace.generate_tokens(
            label=self.config.node.id,
            count=self.config.node.size.value,
        )

        meta = VNodeMeta(
            version=1,
            node_id=self.config.node.id,
            size=self.config.node.size,
            tokens=list(tokens),
        )

        meta.save(path)
        return meta

    def _validate_meta(self, meta: VNodeMeta) -> None:
        if meta.node_id != self.config.node.id:
            raise RuntimeError(
                f"VNodeMeta integrity error: data_dir belongs to node '{meta.node_id}', "
                f"but configuration declares node '{self.config.node.id}'. "
                f"Starting with mismatched vnode ownership would corrupt the ring. "
                f"Fix by restoring the correct data_dir or wiping it before restart."
            )

    def _build_config(self) -> Config:
        config = self.config
        host = config.server.host
        port = config.server.port
        advertised_listener = config.advertised.listener or f"{host}:{port}"

        app_config = Config(
            app=self.app,
            node_id=config.node.id,
            node_size=config.node.size,
            host=host,
            port=port,
            tls_certfile=config.server.tls.certfile,
            tls_keyfile=config.server.tls.keyfile,
            tls_cafile=config.server.tls.cafile,
            backlog=config.server.backlog,
            timeout_graceful_shutdown=config.server.timeout_graceful_shutdown,
            advertised_listener=advertised_listener,
            limit_concurrency=config.server.limit_concurrency,
            max_buffer_size=config.server.max_buffer_size,
            max_message_size=config.server.max_message_size,
            partition_shift=config.placement.shift,
            replication_factor=config.placement.replication_factor
        )

        return app_config

    def _build_storage(self, placement: PlacementStrategy) -> VersionedStorage:
        storage_factory = LMDBStorageFactory(self.config.storage.data_dir)
        partitioned = PartitionedStorage(
            storage_factory=storage_factory,
            placement=placement,
        )
        return VersionedStorage(
            backend=partitioned,
            node_id=self.config.node.id,
        )
