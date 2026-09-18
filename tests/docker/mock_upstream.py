import asyncio
import json
import time

from websockets.asyncio.server import serve


HOST = "0.0.0.0"
START_PORT = 9001
END_PORT = 9100


async def handler(ws):
    port = ws.local_address[1]
    seq = 0

    print(f"[{port}] client connected", flush=True)

    try:
        while True:
            payload = {
                "source_port": port,
                "seq": seq,
                "timestamp": time.time(),
            }

            await ws.send(json.dumps(payload))

            seq += 1

            # 20 msg/sec с каждого source
            await asyncio.sleep(0.05)

    except Exception as e:
        print(
            f"[{port}] disconnected: {e}",
            flush=True
        )


async def main():
    servers = []

    for port in range(
        START_PORT,
        END_PORT + 1
    ):
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


if __name__ == "__main__":
    asyncio.run(main())
