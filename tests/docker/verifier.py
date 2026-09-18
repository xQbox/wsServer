import asyncio
import os
import time

from websockets.asyncio.client import connect


URL = os.getenv(
    "WS_URL",
    "ws://aggregator:33333/feed"
)

MIN_MESSAGES = 200
TEST_SECONDS = 10


async def connect_with_retry():
    deadline = time.monotonic() + 30

    while time.monotonic() < deadline:
        try:
            return await connect(URL)
        except Exception:
            await asyncio.sleep(0.5)

    raise RuntimeError(
        f"Cannot connect to {URL}"
    )


async def main():
    print(f"Connecting to {URL}", flush=True)

    ws = await connect_with_retry()

    print("Connected to aggregator /feed", flush=True)

    count = 0

    deadline = (
        time.monotonic()
        + TEST_SECONDS
    )

    try:
        while time.monotonic() < deadline:
            timeout = deadline - time.monotonic()

            try:
                message = await asyncio.wait_for(
                    ws.recv(),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                break

            count += 1

            if count <= 5:
                print(
                    f"received[{count}]: {message}",
                    flush=True
                )

    finally:
        await ws.close()

    print(
        f"Received {count} messages",
        flush=True
    )

    if count < MIN_MESSAGES:
        raise SystemExit(
            f"FAIL: expected >= {MIN_MESSAGES}, "
            f"received {count}"
        )

    print("PASS: /feed receives data", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
