from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class FTX(OrderBookExchangeFeed):
    name = "ftx"
    key_field = 'market'
    ws_endpoints = {
        WSEndpoint("wss://ftx.com/ws/"): ["lob", "ticker", "trades"]
    }

    ws_channels = {
        "lob": "orderbook",
        "trades": 'trades',
        "ticker": "ticker",
    }

    symbols_endpoint = "https://ftx.com/api/markets"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list['result']:
            # TODO: Futures
            if m['type'] != 'spot':
                continue

            base, quote = m['baseCurrency'], m['quoteCurrency']
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = m['name']
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            for symbol in symbols:
                msg = {
                    "op": "subscribe",
                    "channel": self.get_channel_from_feed(feed),
                    "market": symbol
                }
                await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
