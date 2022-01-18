import websockets
import asyncio
import json
from time import sleep

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

    def receive_as_json(self, data):
        pass

class OkexSocket(ExchangeSocket):
    '''
    Child class to handle the Okex websocket connections
    '''
    def __init__(self):
        super().__init__("wss://ws.okex.com:8443/ws/v5/public")

    def generate_order_book_request(self, symbol):
        request = {}
        request['op'] = 'subscribe'
        request['args'] = [{"channel": "books", "symbol": symbol}]
        return json.dumps(request)

    async def get_order_book_data(self):
        request = self.generate_order_book_request('BTC-USDT')
        await self.ws.send(request)
        data = await self.receive()
        return data

    def normalise(self, data):
        pass

    def receive_as_json(self, data):
        pass

async def main():
    okex = OkexSocket()
    await okex.connect()
    print("Connected")
    while True:
        data = await okex.get_order_book_data()
        print(data)
        sleep(1)

if __name__ == "__main__":
    asyncio.run(main())