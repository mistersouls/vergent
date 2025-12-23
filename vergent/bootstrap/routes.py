
from vergent.bootstrap.deps import get_storage, get_app

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
    return Event(type="pong", payload={"from": "", "epoch": 0})
