import copy
import logging
import uuid
from typing import Callable

from vergent.core.model.event import Event
from vergent.core.router import Router, RouteHandler
from vergent.core.types_ import ReceiveEvent, SendEvent


class App:
    def __init__(self) -> None:
        self.router = Router()
        self._logger = logging.getLogger("vergent.core.app")

    async def __call__(self, receive: ReceiveEvent, send: SendEvent) -> None:
        while True:
            event = await receive()
            if event is None:
                break

            handler = self.router.resolve(event.type)

            if handler is None:
                msg = f"Unknown event type '{event.type}'"
                await send(Event(type="req.error", payload={"message": msg}))
                continue

            try:
                data = dict(event.payload)
                data.setdefault("request_id", str(uuid.uuid4()))
                result = await handler(data)
                if result is not None:
                    await send(result)
            except Exception as exc:
                self._logger.error(f"Error in handler '{event.type}': {exc}", exc_info=exc)
                await send(Event(type="req.error", payload={"message": str(exc)}))

    def request(self, event_type: str) -> Callable[[RouteHandler], RouteHandler]:
        return self.router.request(event_type)
