import asyncio
import ssl
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from vergent.core.model.vnode import SizeClass
from vergent.core.types_ import GatewayProtocol


@dataclass
class Config:
    app: GatewayProtocol

    node_id: str

    host: str
    port: int
    backlog: int

    tls_certfile: Path
    tls_keyfile: Path
    tls_cafile: Path

    advertised_listener: str | None = None

    limit_concurrency: int = 1024
    max_buffer_size: int = 4 * 1024 * 1024  # 4MB
    max_message_size: int = 1 * 1024 * 1024 # 1MB

    timeout_graceful_shutdown: float = 5.0

    partition_shift: int = 16
    node_size: SizeClass = SizeClass.L
    replication_factor: int = 3

    def get_server_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ctx.load_cert_chain(certfile=self.tls_certfile, keyfile=self.tls_keyfile)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.tls_cafile)

        return ctx

    def get_client_ssl_ctx(self) -> ssl.SSLContext:
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.load_cert_chain(certfile=self.tls_certfile, keyfile=self.tls_keyfile)
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.load_verify_locations(cafile=self.tls_cafile)

        return ctx
