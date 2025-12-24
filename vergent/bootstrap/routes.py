
from vergent.bootstrap.deps import get_app, get_advertise_address, get_peer_manager, get_versioned_storage

from vergent.core.model.event import Event
from vergent.core.p2p.conflict import ValueVersion

app = get_app()


@app.request("get")
async def get(data: dict) -> Event:
    storage = get_versioned_storage()
    res = await storage.get(data["key"])
    value = res.decode() if res else None
    return Event(type="ok", payload={"content": value})


@app.request("put")
async def put(data: dict) -> Event:
    storage = get_versioned_storage()
    peer_manager = get_peer_manager()
    key = data["key"]
    version = await storage.put_local(key, data["value"].encode())
    replication = Event(
        type="replicate",
        payload={
            "key": key,
            "version": version.to_dict(),
            "source": get_advertise_address()}
    )
    peer_manager.inject_outgoing(replication)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict()})


@app.request("delete")
async def delete(data: dict) -> Event:
    storage = get_versioned_storage()
    peer_manager = get_peer_manager()
    key = data["key"]
    version = await storage.delete_local(key)
    replication = Event(
        type="replicate",
        payload={
            "key": key,
            "version": version.to_dict(),
            "source": get_advertise_address()}
    )
    peer_manager.inject_outgoing(replication)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict()})


@app.request("ping")
async def ping(data: dict) -> Event:
    return Event(type="pong", payload={"from": get_advertise_address()})


@app.request("gossip")
async def gossip(data: dict) -> Event:
    # warning: peer manager => need to distinguish master and workers
    peer_manager = get_peer_manager()
    peer_manager.inject_incoming(Event(type="gossip", payload=data))
    return Event(type="gossip", payload=peer_manager.snapshot_view())


@app.request("replicate")
async def replicate(data: dict) -> Event:
    storage = get_versioned_storage()
    key = data["key"]
    version = ValueVersion.from_dict(data["version"])
    await storage.apply_remote_version(key, version)
    return Event(type="ok", payload={"hlc": version.hlc.to_dict(), "source": get_advertise_address()})
