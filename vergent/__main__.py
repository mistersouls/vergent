from vergent import App, run
from vergent.bootstrap.deps import get_storage

from vergent.core.model.event import Event
from vergent.core.utils.log import setup_logging

app = App()

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


if __name__ == '__main__':
    setup_logging(level="DEBUG")
    run(app)
