from typing import Protocol, Callable, Awaitable, AsyncIterator

from vergent.core.model.event import Event

ReceiveEvent = Callable[[], Awaitable[Event]]

SendEvent = Callable[[Event], Awaitable[None]]


class GatewayProtocol(Protocol):
    async def __call__(self, receive: ReceiveEvent, send: SendEvent) -> None:
        ...


class Storage(Protocol):
    async def get(self, key: bytes) -> bytes | None:
        ...

    async def put(self, key: bytes, value: bytes) -> None:
        ...

    async def delete(self, key: bytes) -> None:
        ...

    def iter(self, limit: int = -1, batch_size: int = 1024) -> AsyncIterator[tuple[str, bytes]]:
        ...


class StorageFactory(Protocol):
    def create(self, sid: str) -> Storage:
        ...
