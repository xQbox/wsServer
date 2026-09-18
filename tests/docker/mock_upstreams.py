import asyncio
import json
import time

import websockets
from websockets.asyncio.server import serve

HOST = "0.0.0.0"
START_PORT = 9001
END_PORT = 9100


async def handler(ws, path=None):
    port = ws.local_address[1]
    seq = 0

    print(f"[{port}] client connected")

    try:
        while True:
            message = {
                "source_port": port,
                "seq": seq,
                "timestamp": time.time(),
            }

            await ws.send(json.dumps(message))

            seq += 1

            await asyncio.sleep(1)

    except websockets.exceptions.ConnectionClosed:
        print(f"[{port}] client disconnected")


async def main():
    print("Starting mock upstreams...", flush=True)

    servers = []

    for port in range(START_PORT, END_PORT + 1):
        print(f"binding {port}", flush=True)

        server = await serve(
            handler,
            HOST,
            port,
            ping_interval=None,
        )

        servers.append(server)

    print(
        f"READY: {len(servers)} WebSocket servers "
        f"on ports {START_PORT}-{END_PORT}",
        flush=True
    )

    await asyncio.Future()
    servers = []

    for port in range(START_PORT, END_PORT + 1):
        server = await websockets.serve(
            handler,
            HOST,
            port,
        )

        servers.append(server)

    print(
        f"Started {len(servers)} WebSocket servers: "
        f"ws://{HOST}:{START_PORT} ... ws://{HOST}:{END_PORT}"
    )

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
