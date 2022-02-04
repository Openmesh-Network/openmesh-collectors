import websockets
import asyncio
import json


class BybitWebsocket():
    def __init__(self):
        self.msg_queue = asyncio.Queue()
        self.url = 'wss://stream.bybit.com/realtime'
    
    def __repr__(self):
        return f"<BybitWebsocket: url={self.url}>"
    
    def get_ws(self):
        return self.ws

    def get_sub_msg(self):
        symbol = "BTCUSD"
        request = {
            "op": "subscribe",
            "args": ["orderBookL2_25." + symbol, "trade." + symbol]
        }
        return json.dumps(request).encode('utf-8')

    def get_unsub_msg(self):
        symbol = "BTCUSD"
        request = {
            "op": "unsubscribe",
            "args": ["orderBookL2_25." + symbol, "trade." + symbol]
        }
        return json.dumps(request).encode('utf-8')
    
    async def shutdown(self):
        await self.ws.close()

    async def get_msg(self):
        return await self.msg_queue.get()

    async def start_ws(self):
        self.ws = await websockets.connect(self.url)
    
    async def subscribe(self):
        try:
            await self.send(self.get_sub_msg())
        except websockets.ConnectionClosed as e:
            print(f"Websocket Disconnected: {e}")
        except websockets.ConnectionClosedError as e:
            print(f"Websocket Error: {e}")
    
    async def send(self, msg):
        await self.ws.send(msg)

    async def recv(self):
        async for msg in self.ws:
            await self.msg_queue.put(msg)

if __name__ == "__main__":
    ws = BybitWebsocket()
    asyncio.run(ws.start_ws())