from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Dydx(DataFeed):
    name = "dydx"
    sym_field = 'id'
    type_field = 'channel'

    ws_endpoints = {
        WSEndpoint("wss://api.dydx.exchange/v3/ws"): ["trades"]
    }

    ws_channels = {
        "lob": "v3_orderbook",
        "trades": "v3_trades"
    }

    symbols_endpoint = "https://api.dydx.exchange/v3/markets"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for s, e in sym_list['markets'].items():
            if e['status'] != 'ONLINE':
                continue
            base, quote = e['baseAsset'], e['quoteAsset']
            t = e['type'].lower()
            sym = Symbol(base, quote, t)
            ret[sym] = s
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            for symbol in symbols:
                msg = {
                    "type": "subscribe",
                    "id": symbol,
                    "channel": self.get_channel_from_feed(feed)
                }
                if feed == "lob":
                    msg['includeOffsets'] = True
                await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
