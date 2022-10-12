from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Coinbase(OrderBookExchangeFeed):
    name = "coinbase"
    key_field = 'product_id'
    ws_endpoints = {
        WSEndpoint("wss://ws-feed.pro.coinbase.com"): ["lob_l3", "ticker"]
    }

    ws_channels = {
        "lob_l3": "full",
        # Trade messages are sent in the full channel, but for consistency we keep it separate
        "trades_l3": "full",
        "ticker": "ticker"
    }

    symbols_endpoint = "https://api.pro.coinbase.com/products"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        symbols = [s['id'] for s in sym_list]
        for symbol in symbols:
            base, quote = symbol.split("-")
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = symbol
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            if feed == "trades_l3":
                continue
            msg = json.dumps({
                "type": "subscribe",
                "product_ids": symbols,
                "channels": [self.get_channel_from_feed(feed)]
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass
