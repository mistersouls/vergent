import asyncio
import ssl
from dataclasses import dataclass

from vergent.core.types_ import GatewayProtocol


@dataclass
class Config:
    app: GatewayProtocol

    host: str
    port: int
    backlog: int

    loop: asyncio.AbstractEventLoop

    advertise_address: str | None = None

    limit_concurrency: int = 1024
    max_buffer_size: int = 4 * 1024 * 1024  # 4MB
    max_message_size: int = 1 * 1024 * 1024 # 1MB

    timeout_graceful_shutdown: float = 5.0
