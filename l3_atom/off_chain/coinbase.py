from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
import json

class Coinbase(OrderBookExchangeFeed):
    name = "coinbase"
    key_field = 'product_id'
    ws_endpoints = {
        WSEndpoint("wss://ws-feed.pro.coinbase.com"): ["l3_book", "ticker"]
    }

    ws_channels = {
        "l3_book": "full",
        "ticker": "ticker_batch"
    }

    symbols_endpoint = "https://api.pro.coinbase.com/products"
        
    def normalize_symbols(self, sym_list: list) -> dict:
        ret = {}
        symbols = [s['id'] for s in sym_list]
        for symbol in symbols:
            base, quote = symbol.split("-")
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = symbol
        return ret
    
    async def subscribe(self, conn: AsyncFeed, channels: list, symbols):
        print(self.symbols)
        for channel in channels:
            msg = json.dumps({
                "type": "subscribe",
                "product_ids": symbols,
                #"product_ids": syms,
                "channels": [self.get_feed_from_channel(channel)]
            })
            await conn.send_data(msg)
            print(msg)

    def auth(self, conn: WSConnection):
        pass