import asyncio

import uvicorn

from vergent.bootstrap.deps import get_cli_args
from vergent.core.config import Config
from vergent.core.server import Server
from vergent.core.app import App


from vergent.core.types_ import GatewayProtocol
from vergent.core.utils.log import setup_logging


def run(
    app: GatewayProtocol,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    backlog: int = 2048,
    timeout_graceful_shutdown: float = 5.0,
    advertise_address: str | None = None,
    limit_concurrency: int = 1024,
    max_buffer_size: int = 4 * 1024 * 1024,  # 4MB
    max_message_size: int = 1 * 1024 * 1024, # 1MB
    loop: asyncio.AbstractEventLoop | None = None,
):
    if loop is None:
        loop = asyncio.new_event_loop()

    config = Config(
        app=app,
        host=host,
        port=port,
        backlog=backlog,
        timeout_graceful_shutdown=timeout_graceful_shutdown,
        advertise_address=advertise_address,
        limit_concurrency=limit_concurrency,
        max_buffer_size=max_buffer_size,
        max_message_size=max_message_size,
        loop=loop,
    )
    server = Server(config=config)

    try:
        server.run()
    except KeyboardInterrupt:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            loop.close()


def entrypoint(app: App):
    args = get_cli_args()
    setup_logging(args.log_level)

    run(
        app=app,
        host=args.host,
        port=args.port,
        backlog=args.backlog,
        timeout_graceful_shutdown=args.timeout_graceful_shutdown,
        advertise_address=args.advertise_address,
        limit_concurrency=args.limit_concurrency,
        max_buffer_size=args.max_buffer_size,
        max_message_size=args.max_message_size,
    )
