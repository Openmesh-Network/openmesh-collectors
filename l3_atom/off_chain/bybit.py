from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Bybit(OrderBookExchangeFeed):
    name = "bybit"
    ws_endpoints = {
        WSEndpoint("wss://stream.bybit.com/spot/public/v3"): ["lob", "ticker", "candle", "trades"]
    }

    ws_channels = {
        "lob": "orderbook.40.",
        "trades": 'trade.',
        "ticker": "bookticker.",
        "candle": "kline.1m."
    }

    symbols_endpoint = "https://api.bybit.com/spot/v3/public/symbols"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list['result']['list']:
            base, quote = m['baseCoin'], m['quoteCoin']
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = m['name']
        return ret

    @classmethod
    def get_key(cls, msg: dict):
        if 'topic' in msg:
            return msg['topic'].split('.')[-1].encode()

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        args = []
        for feed in feeds:
            args.extend([f"{self.get_channel_from_feed(feed)}{symbol}" for symbol in symbols])
        msg = {
            "op": "subscribe",
            "args": args
        }
        await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
