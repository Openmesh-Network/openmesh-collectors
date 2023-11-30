from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Binance(DataFeed):
    name = "binance"
    sym_field = 's'
    ws_endpoints = {
        WSEndpoint("wss://stream.binance.com:9443/ws"): ["ticker", "candle", "trades_l3"]
    }

    ws_channels = {
        "lob": "depth@100ms",
        "trades_l3": 'trade',
        "ticker": "bookTicker",
        "candle": "kline_1s"
    }

    symbols_endpoint = "https://api.binance.com/api/v3/exchangeInfo"

    @classmethod
    def get_type_from_msg(cls, msg):
        t = msg.get('e', None)
        if not t:
            return 'A' in msg and 'ticker' or None
        return t

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list['symbols']:
            base, quote = m['baseAsset'], m['quoteAsset']
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = m['symbol']
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            msg = json.dumps({
                "method": "SUBSCRIBE",
                "params": [
                    f"{symbol.lower()}@{self.get_channel_from_feed(feed)}"
                    for symbol in symbols
                ],
                "id": 1
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass
