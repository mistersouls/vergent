import functools
import logging
from typing import Callable, Awaitable, Mapping, Any

from vergent.core.model.event import Event

RouteHandler = Callable[[Mapping[str, Any]], Awaitable[Event]]


class Router:
    def __init__(self) -> None:
        self._routes: dict[str, RouteHandler] = {}
        self._logger = logging.getLogger("vergent.core.router")

    def request(self, method: str) -> Callable[[RouteHandler], RouteHandler]:
        def decorator(func: RouteHandler) -> RouteHandler:
            if method in self._routes:
                raise RuntimeError(f"Handler already registered for '{method}'")

            @functools.wraps(func)
            async def wrapper(*args, **kwargs): # keep it for later
                return await func(*args, **kwargs)

            self._routes[method] = wrapper
            return wrapper

        return decorator

    def resolve(self, method: str) -> RouteHandler | None:
        return self._routes.get(method)

    def routes(self) -> dict[str, RouteHandler]:
        return dict(self._routes)
