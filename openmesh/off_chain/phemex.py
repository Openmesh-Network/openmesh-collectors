from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Phemex(DataFeed):
    name = "phemex"
    sym_field = 'symbol'
    ws_endpoints = {
        WSEndpoint("wss://phemex.com/ws"): ["trades", "candle"]
    }

    ws_channels = {
        "lob": "orderbook.subscribe",
        "trades": 'trade.subscribe',
        "candle": 'kline.subscribe'
    }

    symbols_endpoint = "https://api.phemex.com/exchange/public/cfg/v2/products"

    # Phemex sends out integers for prices and quantities, so we need to convert them to floats depending on the individual symbol
    price_decimal_places = {}
    qty_decimal_places = {}

    @classmethod
    def get_type_from_msg(cls, msg):
        for t in ('trades', 'book', 'cline'):
            if t in msg:
                return t

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for s in sym_list['data']['products']:
            if s['status'] != "Listed":
                continue
            base, quote = [x.strip() for x in s['displaySymbol'].split('/')]
            sym_type = s['type'].lower()
            if sym_type == 'perpetualv2':
                sym_type = 'perpetual'
            normalised_symbol = Symbol(base, quote, symbol_type=sym_type)
            ret[normalised_symbol] = s['symbol']

            price_exponent = s.get('priceScale', None)
            qty_exponent = s.get('ratioScale', None)
            # Default is 10^8
            self.price_decimal_places[normalised_symbol] = 10 ** price_exponent if price_exponent else 10 ** 8
            self.qty_decimal_places[normalised_symbol] = 10 ** qty_exponent if qty_exponent else 10 ** 8

        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            for symbol in symbols:
                msg = {
                    "id": 11,
                    "method": self.get_channel_from_feed(feed),
                    "params": [symbol]
                }
                if feed == 'candle':
                    msg['params'].append(60)
                await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
