from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Coinbase(DataFeed):
    name = "coinbase"
    sym_field = 'product_id'
    ws_endpoints = {
        WSEndpoint("wss://ws-feed.pro.coinbase.com"): ["trades_l3", "ticker"]
    }

    ws_channels = {
        "lob_l3": "full",
        # Trade messages are sent in the full channel, but for consistency we keep it separate
        "trades_l3": "match",
        "ticker": "ticker"
    }

    symbols_endpoint = "https://api.pro.coinbase.com/products"

    @classmethod
    def get_type_from_msg(cls, msg):
        if msg['type'] in ('open', 'done', 'change'):
            return 'lob_l3'
        else:
            return msg['type']

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for symbol in sym_list:
            if symbol['status'] != 'online':
                continue
            s = symbol['id']
            base, quote = s.split("-")
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = s
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            msg = json.dumps({
                "type": "subscribe",
                "product_ids": symbols,
                "channels": [self.get_channel_from_feed(feed)]
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass
