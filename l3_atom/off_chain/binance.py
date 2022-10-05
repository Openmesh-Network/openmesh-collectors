from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
import json

class Binance(OrderBookExchangeFeed):
    name = "coinbase"
    key_field = 'product_id'
    ws_endpoints = {
        WSEndpoint("wss://stream.binance.com:9443"): ["l2_book"]
    }

    ws_channels = {
        "l2_book": "depth",
    }

    symbols_endpoint = "/api/v3/exchangeInfo"
        
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