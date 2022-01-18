from exchange_sockets import ExchangeSocket
import json
import websockets
import asyncio
from time import sleep

class OkexSocket(ExchangeSocket):
    '''
    Child class to handle the Okex websocket connections
    '''
    def __init__(self):
        super().__init__("wss://ws.okex.com:8443/ws/v5/public")

    async def subscribe_to_ob(self, symbol):
        request = {}
        request['op'] = 'subscribe'
        request['args'] = [{"channel": "books", "instId": symbol}]
        request = json.dumps(request)
        await self.ws.send(request)
        data = await self.receive()
        return data

    async def subscribe_to_trades(self, symbol):
        request = {}
        request['op'] = 'subscribe'
        request['args'] = [{"channel": "trades", "instId": symbol}]
        request = json.dumps(request)
        await self.ws.send(request)
        data = await self.receive()
        return data

    def normalise_order_book(self, data):
        data = data['data'][0]
        asks, bids = data['asks'], data['bids']
        #print(asks, bids)
        asks = [{'price': float(ask[0]), 'size': float(ask[1]), 'side': 2} for ask in asks]
        bids = [{'price': float(bid[0]), 'size': float(bid[1]), 'side': 1} for bid in bids]
        asks.extend(bids)
        return asks

    def normalise_trades(self, data):
        trade = data['data'][0]
        trades = {"price" : float(trade['px']), "size" : float(trade['sz']), "side" : 1 if trade['side'] == 'buy' else 2, "time" : trade['ts']}
        return trades


async def main():
    okex = OkexSocket()
    await okex.connect()
    print("Connected")
    await okex.subscribe_to_ob("BTC-USDT")
    print("Subscribed to order book")
    await okex.subscribe_to_trades("BTC-USDT")
    print("Subscribed to trades")
    while True:
        data = json.loads(await okex.receive())
        #print(data)
        if 'event' in data:
            continue
        if data['arg']['channel'] == 'books':
            data = okex.normalise_order_book(data)
            print(json.dumps(data, indent=4))
        elif data['arg']['channel'] == 'trades':
            data = okex.normalise_trades(data)
            print(json.dumps(data, indent=4))
        sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())