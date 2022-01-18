import websockets
import asyncio

class ExchangeSocket():
    '''
    Parent class to handle the exchange websocket connections
    '''
    def __init__(self, url):
        self.url = url
        self.ws = None

    async def connect(self):
        self.ws = await websockets.connect(self.url)
        
    async def receive(self):
        return await self.ws.recv()

    async def close(self):
        await self.ws.close()

    def normalise(self, data):
        pass
