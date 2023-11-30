from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Bybit(DataFeed):
    name = "bybit"
    ws_endpoints = {
        WSEndpoint("wss://stream.bybit.com/spot/public/v3"): ["ticker", "candle", "trades"]
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

    # This results in a different format than the other keys, but we don't actually care about the format -- just that it groups symbols and message types together
    @classmethod
    def get_key(cls, msg: dict):
        if 'topic' in msg:
            return f'{cls.name}_{msg["topic"]}'.encode()

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        args = []
        for feed in feeds:
            args.extend(
                [f"{self.get_channel_from_feed(feed)}{symbol}" for symbol in symbols])
        msg = {
            "op": "subscribe",
            "args": args
        }
        await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
