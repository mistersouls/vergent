
from vergent.bootstrap.deps import get_storage, get_app, get_advertise_address, get_peer_manager

from vergent.core.model.event import Event


app = get_app()


@app.request("get")
async def get(data: dict) -> Event:
    storage = get_storage()
    res = await storage.get(data["key"])
    value = res.decode() if res else None
    return Event(type="ok", payload={"content": value})


@app.request("put")
async def get(data: dict) -> Event:
    storage = get_storage()
    await storage.put(data["key"], data["value"].encode())
    return Event(type="ok", payload={})


@app.request("delete")
async def get(data: dict) -> Event:
    storage = get_storage()
    await storage.delete(data["key"])
    return Event(type="ok", payload={})


@app.request("ping")
async def ping(data: dict) -> Event:
    return Event(type="pong", payload={"from": get_advertise_address()})


@app.request("gossip")
async def gossip(data: dict) -> Event:
    # warning: peer manager => need to distinguish master and workers
    peer_manager = get_peer_manager()
    peer_manager.inject_incoming(Event(type="gossip", payload=data))
    return Event(type="gossip", payload=peer_manager.snapshot_view())
