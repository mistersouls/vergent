import asyncio

from vergent.bootstrap.deps import get_core
from vergent.bootstrap.pkg import scan
from vergent.core.config import Config
from vergent.core.model.state import ServerState
from vergent.core.server import Server


def _shutdown_p2p(state: ServerState, task: asyncio.Task[None]) -> None:
    # todo(souls): move
    state.tasks.discard(task)
    state.stop_event.set()


def run(
    # app: GatewayProtocol,
    *,
    config: Config,
    loop: asyncio.AbstractEventLoop | None = None,
):
    if loop is None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    server = Server(config=config, loop=loop)

    # peer_manager = get_peer_manager()
    # p2p_task = loop.create_task(peer_manager.manage(server.state.stop_event))
    # p2p_task.add_done_callback(functools.partial(_shutdown_p2p, server.state))
    # server.state.tasks.add(p2p_task)

    try:
        server.run()
    except KeyboardInterrupt:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            loop.close()


@scan("vergent.bootstrap.handlers")
def entrypoint() -> None:
    core = get_core()
    run(config=core.config, loop=core.loop)
