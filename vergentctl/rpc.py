import struct
import msgpack
from vergent.core.model.event import Event

async def send_event(writer, event: Event) -> None:
    payload = msgpack.packb(event.to_dict(), use_bin_type=True)
    frame = struct.pack("!I", len(payload)) + payload
    writer.write(frame)
    await writer.drain()

async def read_event(reader) -> Event:
    header = await reader.readexactly(4)
    length = struct.unpack("!I", header)[0]
    payload = await reader.readexactly(length)
    data = msgpack.unpackb(payload, raw=False)
    return Event(**data)

async def rpc_call(reader, writer, event: Event) -> Event:
    await send_event(writer, event)
    return await read_event(reader)
