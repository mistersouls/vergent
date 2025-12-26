import shlex
from vergent.core.model.event import Event
from vergentctl.parser import parse_value, ParseError, validate_key, split_key_value
from vergentctl.rpc import rpc_call


COMMANDS = {}

def command(name):
    def decorator(fn):
        COMMANDS[name] = fn
        return fn
    return decorator


@command("get")
async def cmd_get(reader, writer, args: str):
    parts = shlex.split(args)

    if len(parts) != 1:
        raise ParseError("Usage: get <key>")

    key = validate_key(parts[0])
    event = Event("get", {"key": key})
    return await rpc_call(reader, writer, event)


@command("put")
async def cmd_put(reader, writer, args: str):
    """
    Handle the 'put' command.

    Expected syntax:
        put <key> <value>

    Rules:
      - The key may contain spaces and must be quoted if so.
      - The key is extracted using split_key_value().
      - The value is the remaining raw string and is parsed by parse_value().
      - The key is validated only for UTF-8 and size constraints.
    """
    key, value_str = split_key_value(args)

    if not value_str:
        raise ParseError("Usage: put <key> <value>")

    key = validate_key(key)
    value = parse_value(value_str)
    event = Event("put", {"key": key, "value": value})
    return await rpc_call(reader, writer, event)



@command("delete")
async def cmd_delete(reader, writer, args: str):
    parts = shlex.split(args)

    if len(parts) != 1:
        raise ParseError("Usage: delete <key>")

    key = validate_key(parts[0])
    event = Event("delete", {"key": key})
    return await rpc_call(reader, writer, event)
