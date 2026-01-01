from vergent.bootstrap.deps import get_app, get_core
from vergent.core.model.event import Event

app = get_app()


@app.request("ping")
async def ping(data: dict) -> Event:
    core = get_core()
    return Event(type="pong", payload={"from": core.config.advertised_listener})


@app.request("gossip")
async def gossip(data: dict) -> Event:
    # warning: peer manager => need to distinguish master and workers
    # peer_manager = get_peer_manager()
    # peer_manager.inject_incoming(Event(type="gossip", payload=data))
    snapshot = {}
    return Event(type="gossip", payload=snapshot)
