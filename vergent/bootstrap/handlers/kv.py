import msgpack

from vergent.bootstrap.deps import (
    get_app,
    get_advertise_address,
    get_peer_manager,
    get_core
)

from vergent.core.model.event import Event
from vergent.core.p2p.conflict import ValueVersion

app = get_app()


@app.request("get")
async def get(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    res = await storage.get(data["key"])
    if res is not None:
        value = msgpack.unpackb(res, raw=False)
    else:
        value = None

    return Event(type="ok", payload={"content": value})


@app.request("put")
async def put(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    # peer_manager = get_peer_manager()
    key = data["key"]
    value = msgpack.packb(data["value"], use_bin_type=True)
    version = await storage.put_local(key, value)
    # replication = Event(
    #     type="replicate",
    #     payload={
    #         "key": key,
    #         "version": version.to_dict(),
    #         "source": get_advertise_address()}
    # )
    # peer_manager.inject_outgoing(replication)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict()})


@app.request("delete")
async def delete(data: dict) -> Event:
    core = get_core()
    storage = core.storage
    # peer_manager = get_peer_manager()
    key = data["key"]
    version = await storage.delete_local(key)
    # replication = Event(
    #     type="replicate",
    #     payload={
    #         "key": key,
    #         "version": version.to_dict(),
    #         "source": get_advertise_address()}
    # )
    # peer_manager.inject_outgoing(replication)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict()})
