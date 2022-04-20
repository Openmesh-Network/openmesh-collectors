import websockets
import asyncio
import time

async def connect(url, handle_connection, *args):
    async for ws in websockets.connect(url):
        try:
            t0 = time.time()
            await handle_connection(ws, *args)
        except websockets.ConnectionClosedError:
            t1 = time.time()
            print(f"{t1 - t0} seconds elasped before disconnection")
            continue