import msgpack

from vergent.bootstrap.deps import get_app, get_core
from vergent.core.model.event import Event


app = get_app()


@app.request("get")
async def get(data: dict) -> Event:
    core = get_core()
    res = await core.coordinator.get(data["key"].encode(), 1, 0.05)
    if res is not None:
        value = msgpack.unpackb(res, raw=False)
    else:
        value = None

    return Event(type="ok", payload={"content": value})


@app.request("put")
async def put(data: dict) -> Event:
    core = get_core()
    key = data["key"].encode()
    value = msgpack.packb(data["value"], use_bin_type=True)
    ok = await core.coordinator.put(key, value, 1, 0.05)
    return Event(type="ok", payload={"hlc": "todo", "ok": ok})


@app.request("delete")
async def delete(data: dict) -> Event:
    core = get_core()
    key = data["key"].encode()
    ok = await core.coordinator.delete(key, 1, 0.05)
    return Event(type="ok", payload={"hlc": "todo", "ok": ok})
