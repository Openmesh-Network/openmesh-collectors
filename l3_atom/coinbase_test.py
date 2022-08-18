from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
import json
import asyncio

class Coinbase(OrderBookExchangeFeed):
    name = "coinbase"
    ws_endpoints = {
        WSEndpoint("wss://ws-feed.pro.coinbase.com"): ["l3_book", "trades"]
    }

    ws_channels = {
        "l3_book": "full",
        "trades": "matches"
    }

    def normalize_symbols(self, symbols: list) -> dict:
        ret = {}
        for symbol in symbols:
            base, quote = symbol.split("-")
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = symbol
        return ret

    def __init__(self):
        super().__init__()

    async def _trade(self, msg, timestamp):
        pair = self.get_normalised_symbol(msg["product_id"])
    
    async def subscribe(self, conn: AsyncFeed, channels: list):
        for channel in channels:
            msg = json.dumps({
                "type": "subscribe",
                "product_ids": list(self.symbols.values()),
                "channels": [self.get_feed_from_channel(channel)]
            })
            await conn.send_data(msg)
            print(msg)

    def auth(self, conn: WSConnection):
        pass

    
if __name__ == "__main__":
    main()
