from openmesh.off_chain import Binance
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, HTTPConnection


class BinanceFutures(Binance):
    name = "binance-futures"
    ws_endpoints = {
        WSEndpoint("wss://fstream.binance.com/ws"): ["ticker", "candle", "trades", "funding_rate"]
    }

    rest_endpoints = {
        'https://fapi.binance.com': ['open_interest']
    }

    ws_channels = {
        "lob": "depth@100ms",
        "trades": "aggTrade",
        "ticker": "bookTicker",
        "candle": "kline_1s",
        "funding_rate": "markPrice@1s"
    }

    rest_channels = {
        'open_interest': 'https://fapi.binance.com/fapi/v1/openInterest?symbol={}'
    }

    symbols_endpoint = "https://fapi.binance.com/fapi/v1/exchangeInfo"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list['symbols']:
            base, quote = m['baseAsset'], m['quoteAsset']
            market = 'spot'
            expiration_date = None
            if m.get('contractType') == 'PERPETUAL':
                market = 'perpetual'
            elif m.get('contractType') == 'CURRENT_QUARTER' or m.get('contractType') == 'NEXT_QUARTER':
                market = 'futures'
                expiration_date = m['symbol'].split("_")[1]
            normalised_symbol = Symbol(
                base, quote, symbol_type=market, expiry_date=expiration_date)
            ret[normalised_symbol] = m['symbol']
        return ret

    def _init_rest(self):
        return [HTTPConnection(self.name, self.rest_channels['open_interest'].format(self.get_exchange_symbol(symbol)), poll_frequency=60, authentication=None) for symbol in self.symbols]

    def auth(self, conn: WSConnection):
        pass
