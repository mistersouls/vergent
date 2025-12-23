import asyncio


def get_remote_addr(transport: asyncio.Transport) -> tuple[str, int] | None:
    socket = transport.get_extra_info('socket')
    if socket is not None:
        info = socket.getpeername()
    else:
        info = transport.get_extra_info('peername')

    if len(info) == 2:
        return info[0], info[1]
    return None


def get_local_addr(transport: asyncio.Transport) -> tuple[str, int] | None:
    socket = transport.get_extra_info('socket')
    if socket is not None:
        info = socket.getsocketname()
    else:
        info = transport.get_extra_info('sockname')

    if len(info) == 2:
        return info[0], info[1]
    return None
