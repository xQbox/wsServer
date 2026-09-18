import asyncio
import json
import websockets


async def handler(ws):
    counter = 0

    while True:
        counter += 1

        payload = {
            "status": "online",
            "message_id": counter
        }

        await ws.send(json.dumps(payload))
        print("sent", counter)

        await asyncio.sleep(1)


async def main():
    async with websockets.serve(
        handler,
        "127.0.0.1",
        50001
    ):
        print("WS server listening on 127.0.0.1:50000")
        await asyncio.Future()


asyncio.run(main())
