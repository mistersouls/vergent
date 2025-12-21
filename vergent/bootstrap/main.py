import asyncio

from vergent.core.app import App
from vergent.core.config import Config
from vergent.core.server import Server


def run(
    app: App,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    backlog: int = 2048,
    loop: asyncio.AbstractEventLoop | None = None,
):
    if loop is None:
        loop = asyncio.new_event_loop()

    config = Config(
        app=app,
        host=host,
        port=port,
        backlog=backlog,
        loop=loop,
    )

    server = Server(config=config)
    server.run()
