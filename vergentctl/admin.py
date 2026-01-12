import asyncio
import ssl

from vergent.core.model.event import Event
from vergentctl.rpc import send_event, read_event


def get_ssl_ctx(certfile: str, keyfile: str, cafile: str) -> ssl.SSLContext:
    ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    ssl_ctx.load_verify_locations(cafile=cafile)
    return ssl_ctx


async def admin_cmd(event: Event, host: str, port: int, certfile: str, keyfile: str, cafile: str) -> None:
    ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ssl_ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
    ssl_ctx.verify_mode = ssl.CERT_REQUIRED
    ssl_ctx.load_verify_locations(cafile=cafile)

    reader, writer = await asyncio.open_connection(
        host, port, ssl=ssl_ctx
    )

    await send_event(writer, event)
    resp = await read_event(reader)
    print(resp)
