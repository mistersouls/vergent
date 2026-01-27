import asyncio
import ssl
from vergent.bootstrap.config.settings import VergentConfig
from vergent.core.app import App
from vergent.core.bucket import BucketTable
from vergent.core.config import ApiConfig, PeerConfig, ReplicationConfig
from vergent.core.coordinator import Coordinator
from vergent.core.facade import VergentCore
from vergent.core.model.membership import Membership
from vergent.core.model.partition import Partitioner
from vergent.core.model.state import PeerState, NodeMeta
from vergent.core.model.vnode import VNode
from vergent.core.p2p.connection import PeerConnectionPool
from vergent.core.p2p.hlc import HLC
from vergent.core.ports.node import NodeMetaStore
from vergent.core.replication import PartitionTransfer
from vergent.core.ring import Ring
from vergent.core.space import HashSpace
from vergent.core.storage.partitionned import PartitionedStorage
from vergent.core.storage.versionned import VersionedStorage
from vergent.core.sub import Subscription
from vergent.infra.local_storage import LMDBStorageFactory
from vergent.infra.nodemeta_store import SafeNodeMetaStore


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
        meta_store = self._build_meta_store()
        node_meta = meta_store.get()
        self._validate_meta(node_meta)

        server_ssl_ctx = self.config.get_server_ssl_ctx()
        client_ssl_ctx = self.config.get_client_ssl_ctx()
        api_config = self._build_api_config(server_ssl_ctx)
        peer_config = self._build_peer_config(server_ssl_ctx, client_ssl_ctx)
        replication_config = self._build_replication_config(server_ssl_ctx)


        peer_state = self._build_peer_state(node_meta, peer_config)

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
            peers=peer_clients,
            state=peer_state,
            partitioner=partitioner,
            subscription=subscription,
            storage=storage,
            replication_factor=self.config.placement.replication_factor,
            loop=loop
        )

        pts = PartitionTransfer(
            ssl_ctx=client_ssl_ctx,
            storage=storage,
            loop=loop
        )

        return VergentCore(
            api_config=api_config,
            peer_config=peer_config,
            replication_config=replication_config,
            peer_state=peer_state,
            meta_store=meta_store,
            incoming=subscription,
            view=BucketTable(128),
            partitioner=partitioner,
            storage=storage,
            coordinator=coordinator,
            pts=pts,
            peer_clients=peer_clients,
            loop=loop
        )

    def _build_meta_store(self) -> NodeMetaStore:
        path = self.config.storage.data_dir / "meta" / "node.meta"
        store = SafeNodeMetaStore(path)

        if path.exists():
            return store

        node_id = self.config.node.id
        size = self.config.node.size
        meta = NodeMeta(
            node_id=node_id,
            size=size,
            tokens=[],
            hlc=HLC.initial(node_id)
        )
        store.save(meta)
        return store

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
        replication = server.replication
        peer_host = peer.host
        peer_port = peer.port
        advertised_listener = peer.listener or f"{peer.host}:{peer.port}"
        replication_listener = replication.listener or f"{replication.host}:{replication.port}"

        peer_config = PeerConfig(
            app=self.peer,
            node_id=self.config.node.id,
            node_size=self.config.node.size,
            host=peer_host,
            port=peer_port,
            backlog=server.backlog,
            ssl_ctx=server_ssl_ctx,
            client_ssl_ctx=client_ssl_ctx,
            timeout_graceful_shutdown=server.timeout_graceful_shutdown,
            limit_concurrency=server.limit_concurrency,
            max_buffer_size=server.max_buffer_size,
            max_message_size=server.max_message_size,
            peer_listener=advertised_listener,
            replication_listener=replication_listener,
            seeds=set(peer.seeds),
            partition_shift=self.config.placement.shift,
            replication_factor=self.config.placement.replication_factor
        )
        return peer_config

    def _build_replication_config(self, server_ssl_ctx: ssl.SSLContext) -> ReplicationConfig:
        server = self.config.server
        replication = server.replication
        return ReplicationConfig(
            host=replication.host,
            port=replication.port,
            backlog=server.backlog,
            ssl_ctx=server_ssl_ctx,
            timeout_graceful_shutdown=server.timeout_graceful_shutdown,
            limit_concurrency=server.limit_concurrency,
            max_buffer_size=server.max_buffer_size,
            max_message_size=server.max_message_size,
        )

    @staticmethod
    def _build_peer_state(meta: NodeMeta, config: PeerConfig) -> PeerState:
        state = PeerState(
            ring=Ring(),
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

    def _validate_meta(self, meta: NodeMeta) -> None:
        if meta.node_id != self.config.node.id:
            raise RuntimeError(
                f"VNodeMeta integrity error: data_dir belongs to node '{meta.node_id}', "
                f"but configuration declares node '{self.config.node.id}'. "
                f"Starting with mismatched vnode ownership would corrupt the ring. "
                f"Fix by restoring the correct data_dir or wiping it before restart."
            )
