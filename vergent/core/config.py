import ssl
from dataclasses import dataclass

from vergent.core.model.vnode import SizeClass
from vergent.core.types_ import GatewayProtocol


@dataclass
class ServerConfig:
    host: str
    port: int
    backlog: int

    ssl_ctx: ssl.SSLContext

    limit_concurrency: int = 1024
    max_buffer_size: int = 4 * 1024 * 1024  # 4MB
    max_message_size: int = 1 * 1024 * 1024  # 1MB

    timeout_graceful_shutdown: float = 5.0


@dataclass(kw_only=True)
class ApiConfig(ServerConfig):
    app: GatewayProtocol


@dataclass(kw_only=True)
class PeerConfig(ServerConfig):
    app: GatewayProtocol
    node_id: str
    seeds: set[str]
    peer_listener: str
    replication_listener: str
    client_ssl_ctx: ssl.SSLContext
    partition_shift: int = 16
    node_size: SizeClass = SizeClass.L
    replication_factor: int = 3


@dataclass(kw_only=True)
class ReplicationConfig(ServerConfig):
    max_concurrent_transfers: int = 4
    timeout_transfers: float = 3600.0
