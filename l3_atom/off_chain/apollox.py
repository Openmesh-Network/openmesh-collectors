from l3_atom.off_chain import BinanceFutures
from l3_atom.feed import WSEndpoint


class ApolloX(BinanceFutures):
    name = "apollox"

    ws_endpoints = {
        WSEndpoint("wss://fstream.apollox.finance/ws"): ["lob", "ticker", "candle", "trades_l3"]
    }

    rest_channels = {
        'open_interest': 'https://fapi.apollox.finance/fapi/v1/openInterest?symbol={}'
    }

    symbols_endpoint = "https://fapi.apollox.finance/fapi/v1/exchangeInfo"
