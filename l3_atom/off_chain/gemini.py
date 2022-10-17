from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from l3_atom.helpers.read_config import get_conf_symbols
from yapic import json
import requests


class Gemini(OrderBookExchangeFeed):
    name = "gemini"
    key_field = 'symbol'
    ws_endpoints = {
        WSEndpoint("wss://api.gemini.com/v2/marketdata"): ["lob", "candle"]
    }

    ws_channels = {
        "lob": "l2",
        "trades": 'l2',
        "candle": "candles_1m"
    }

    symbols_endpoint = "https://api.gemini.com/v1/symbols"
    symbols_info_endpoint = "https://api.gemini.com/v1/symbols/details/{}"

    # Gemini requires us to query each symbol information separately, to avoid rate limits let's only request the symbols we need
    def _get_sym_filters(self):
        conf_symbols = get_conf_symbols(self.name)
        return [f'{"".join(sym.split("."))}' for sym in conf_symbols]

    def get_symbols(self):
        sym_list = self._get_sym_filters()
        ret = []
        for sym in sym_list:
            sym_info = requests.get(
                self.symbols_info_endpoint.format(sym)).json()
            ret.append(sym_info)
        return ret

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list:
            base, quote = m['base_currency'], m['quote_currency']
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = m['symbol']
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            msg = json.dumps({
                "type": "subscribe",
                "subscriptions": [{
                    "name": self.get_channel_from_feed(feed),
                    "symbols": symbols
                }]
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, channel: str):
        print(message)
        await self.kafka_connector.write(message)
