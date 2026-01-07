import asyncio
import ssl
from pathlib import Path

from vergent.bootstrap.config.meta import VNodeMeta
from vergent.bootstrap.config.settings import VergentConfig
from vergent.core.app import App
from vergent.core.bucket import BucketTable
from vergent.core.config import ApiConfig, PeerConfig
from vergent.core.coordinator import Coordinator
from vergent.core.facade import VergentCore
from vergent.core.model.membership import Membership
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.ring import Ring
from vergent.core.storage.partitionned import PartitionedStorage
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription
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

    def __init__(self, api: App, peer: App, config: VergentConfig) -> None:
        self.config = config
        self.api = api
        self.peer = peer

    def build(self) -> VergentCore:
        server_ssl_ctx = self.config.get_server_ssl_ctx()
        client_ssl_ctx = self.config.get_client_ssl_ctx()
        api_config = self._build_api_config(server_ssl_ctx)
        peer_config = self._build_peer_config(server_ssl_ctx, client_ssl_ctx)


        peer_state = self._build_peer_state(peer_config)

        partitioner = Partitioner(self.config.placement.shift)
        storage = self._build_storage(partitioner)
        loop = self._create_event_loop()

        subscription = Subscription(loop)
        peer_clients = PeerConnectionPool(
            subscription=subscription,
            ssl_ctx=client_ssl_ctx,
            loop=loop
        )

        coordinator = Coordinator(
            node_id=self.config.node.id,
            peers=peer_clients,
            state=peer_state,
            partitioner=partitioner,
            subscription=subscription,
            storage=storage,
            replication_factor=self.config.placement.replication_factor,
            loop=loop
        )

        return VergentCore(
            api_config=api_config,
            peer_config=peer_config,
            peer_state=peer_state,
            incoming=subscription,
            view=BucketTable(128),
            partitioner=partitioner,
            storage=storage,
            coordinator=coordinator,
            peer_clients=peer_clients,
            loop=loop
        )

    def _meta_path(self) -> Path:
        return self.config.storage.data_dir / "meta" / "vnodes.meta"

    def _load_or_generate_meta(self) -> VNodeMeta:
        path = self._meta_path()

        if path.exists():
            meta = VNodeMeta.load(path)
            self._validate_meta(meta)
            return meta

        meta = VNodeMeta(
            version=1,
            node_id=self.config.node.id,
            size=self.config.node.size,
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

    def _build_api_config(self, ssl_ctx: ssl.SSLContext) -> ApiConfig:
        server = self.config.server

        api_config = ApiConfig(
            app=self.api,
            host=server.api.host,
            port=server.api.port,
            backlog=server.backlog,
            ssl_ctx=ssl_ctx,
            limit_concurrency=server.limit_concurrency,
            max_buffer_size=server.max_buffer_size,
            max_message_size=server.max_message_size,
            timeout_graceful_shutdown=server.timeout_graceful_shutdown
        )

        return api_config

    def _build_peer_config(
        self,
        server_ssl_ctx: ssl.SSLContext,
        client_ssl_ctx: ssl.SSLContext
    ) -> PeerConfig:
        server = self.config.server
        peer = server.peer
        host = peer.host
        port = peer.port
        advertised_listener = peer.listener or f"{host}:{port}"

        peer_config = PeerConfig(
            app=self.peer,
            node_id=self.config.node.id,
            node_size=self.config.node.size,
            host=host,
            port=port,
            backlog=server.backlog,
            ssl_ctx=server_ssl_ctx,
            client_ssl_ctx=client_ssl_ctx,
            timeout_graceful_shutdown=server.timeout_graceful_shutdown,
            limit_concurrency=server.limit_concurrency,
            max_buffer_size=server.max_buffer_size,
            max_message_size=server.max_message_size,
            advertised_listener=advertised_listener,
            seeds=set(peer.seeds),
            partition_shift=self.config.placement.shift,
            replication_factor=self.config.placement.replication_factor
        )
        return peer_config

    def _build_peer_state(self, config: PeerConfig) -> PeerState:
        meta = self._load_or_generate_meta()
        vnodes = VNode.generate_vnodes(meta.node_id, meta.size)
        ring = Ring(vnodes)
        membership = Membership(
            node_id=meta.node_id,
            address=config.advertised_listener,
            size=meta.size,
            # state="dead"
        )
        state = PeerState(
            membership=membership,
            vnodes=vnodes,
            ring=ring,
        )
        return state

    def _build_storage(self, partitioner: Partitioner) -> VersionedStorage:
        storage_factory = LMDBStorageFactory(self.config.storage.data_dir)
        partitioned = PartitionedStorage(
            storage_factory=storage_factory,
            partitioner=partitioner,
        )
        return VersionedStorage(
            backend=partitioned,
            node_id=self.config.node.id,
        )

    @staticmethod
    def _create_event_loop() -> asyncio.AbstractEventLoop:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
