from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json
import logging


class Kraken(DataFeed):
    name = "kraken"
    sym_field = -2
    type_field = -3
    ws_endpoints = {
        WSEndpoint("wss://ws.kraken.com"): ["ticker", 'trades', 'candle']
    }

    ws_channels = {
        "lob": "book",
        "trades": "trade",
        "candle": "ohlc",
        "ticker": "ticker"
    }

    symbols_endpoint = "https://api.kraken.com/0/public/AssetPairs"

    @classmethod
    def _get_field(cls, msg, field):
        if field and isinstance(field, str):
            key = msg.get(field, None)
        else:
            try:
                key = msg[field]
            except (IndexError, KeyError):
                if 'event' in msg:
                    return None
                logging.warning(
                    f"Key field {field} not found in message")
                key = None
        return key

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        res = sym_list['result']

        for s in res:
            sym = res[s]
            name = sym.get('wsname', None)
            if name is None:
                return
            rep_name = name.replace('XBT', 'BTC').replace('XDG', 'DOGE')
            base, quote = rep_name.split('/')
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = name
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            channel = self.get_channel_from_feed(feed)
            subscription = {"name": channel}
            if feed == 'lob':
                subscription['depth'] = 1000
            elif feed == 'candle':
                subscription['interval'] = 1
            msg = json.dumps({
                "event": "subscribe",
                "pair": symbols,
                "subscription": subscription
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass
