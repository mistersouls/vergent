import asyncio
from vergentctl.commands import COMMANDS


async def repl(host: str, port: int) -> None:
    reader, writer = await asyncio.open_connection(host, port)

    print(f"Connected to vergent node at {host}:{port}")
    prompt = f"vergent({host}:{port})> "

    while True:
        try:
            line = input(prompt).strip()
        except EOFError:
            break

        if not line:
            continue

        parts = line.split(maxsplit=1)
        op = parts[0]

        if op in ("quit", "exit"):
            break

        if op not in COMMANDS:
            print("Unknown command. Available:")
            for name in COMMANDS:
                print(" ", name)
            continue

        args = parts[1]

        try:
            print(args)
            res = await COMMANDS[op](reader, writer, args)
            if res is not None:
                print(res.payload)
        except Exception as exc:
            print("Error:", exc)

    writer.close()
    await writer.wait_closed()
