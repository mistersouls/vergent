import asyncio
from typing import TYPE_CHECKING
from dataclasses import dataclass, field


if TYPE_CHECKING:
    from vergent.core.protocol import Protocol


@dataclass
class ServerState:
    connections: set[Protocol] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)
