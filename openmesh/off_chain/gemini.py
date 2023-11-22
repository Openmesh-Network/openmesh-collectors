from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json
import requests
import logging


class Gemini(DataFeed):
    name = "gemini"
    sym_field = 'symbol'
    type_field = 'type'
    ws_endpoints = {
        WSEndpoint("wss://api.gemini.com/v2/marketdata"): ["lob", "candle"]
    }

    ws_channels = {
        "lob": "l2",
        "trades": "l2",
        "candle": "candles_1m"
    }

    symbols_endpoint = "https://api.gemini.com/v1/symbols"
    symbols_info_endpoint = "https://api.gemini.com/v1/symbols/details/{}"

    def __init__(self, symbols=None, retries=3, interval=30, timeout=120, delay=0):
        selected_syms = symbols if symbols else []
        for sym in selected_syms:
            logging.info(f"{self.name} - using symbol {sym}")
        self.max_syms = None
        sym_list = self.get_symbols(selected_syms)
        self.symbols = self.normalise_symbols(sym_list)
        self.inv_symbols = {v: k for k, v in self.symbols.items()}
        if selected_syms:
            self.filter_symbols(self.symbols, selected_syms)

        self.connection_handlers = []
        self.retries = retries
        self.interval = interval
        self.timeout = timeout
        self.delay = delay
        self.kafka_connector = None
        self.num_messages = 0
        self.tot_latency = 0

    def _get_sym_filters(self, sym_list):
        return [f'{"".join(sym.split("."))}' for sym in sym_list]

    # Gemini requires us to query each symbol information separately, to avoid rate limits let's only request the symbols we need
    def get_symbols(self, sym_list):
        sym_filters = self._get_sym_filters(sym_list)
        ret = []
        for sym in sym_filters:
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
