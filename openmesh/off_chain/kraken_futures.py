from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class KrakenFutures(DataFeed):
    name = "kraken-futures"
    sym_field = 'product_id'
    type_field = 'feed'
    ws_endpoints = {
        WSEndpoint("wss://futures.kraken.com/ws/v1"): ['trades', 'ticker']
    }

    ws_channels = {
        "lob": "book",
        "trades": "trade",
        "ticker": "ticker",
        "funding_rate": "ticker",
        "open_interest": "ticker",
    }

    symbols_endpoint = "https://futures.kraken.com/derivatives/api/v3/instruments"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        res = sym_list['instruments']

        for s in res:
            if s['tradeable'] is False:
                continue
            temp = s['symbol'].upper().split('_')
            if len(temp) == 3:
                _, sym, expiry_date = temp
                t = 'futures'
            else:
                _, sym = temp
                expiry_date = None
                t = 'perpetual'

            sym = sym.replace('XBT', 'BTC').replace('XDG', 'DOGE')

            base, quote = sym[:3], sym[3:]

            normalised_symbol = Symbol(
                base, quote, expiry_date=expiry_date, symbol_type=t)
            ret[normalised_symbol] = s['symbol'].upper()
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            await conn.send_data(json.dumps({
                "event": "subscribe",
                "feed": self.get_channel_from_feed(feed),
                "product_ids": symbols
            }))

    def auth(self, conn: WSConnection):
        pass
