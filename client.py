import asyncio
import time

import msgpack
import struct


HOST = "127.0.0.1"
PORT = 8000


async def send_message(writer, msg):
    payload = msgpack.packb(msg, use_bin_type=True)
    frame = struct.pack("!I", len(payload)) + payload
    writer.write(frame)
    await writer.drain()


async def read_message(reader):
    # Read length longueur
    header = await reader.readexactly(4)
    length = struct.unpack("!I", header)[0]

    # Read payload
    payload = await reader.readexactly(length)
    return msgpack.unpackb(payload, raw=False)


async def main():
    reader, writer = await asyncio.open_connection(HOST, PORT)

    start = time.time()

    msg = {
        "type": "get",
        "payload": {"key": "souls"}
    }

    print("→ Sending:", msg)
    await send_message(writer, msg)

    response = await read_message(reader)
    print("← Received:", response)

    writer.close()
    await writer.wait_closed()
    print(time.time() - start)


if __name__ == "__main__":
    asyncio.run(main())
