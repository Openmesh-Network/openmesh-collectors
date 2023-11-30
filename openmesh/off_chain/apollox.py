from openmesh.off_chain import BinanceFutures
from openmesh.feed import WSEndpoint


class ApolloX(BinanceFutures):
    name = "apollox"

    ws_endpoints = {
        WSEndpoint("wss://fstream.apollox.finance/ws"): ["ticker", "candle", "trades"]
    }

    rest_channels = {
        'open_interest': 'https://fapi.apollox.finance/fapi/v1/openInterest?symbol={}'
    }

    symbols_endpoint = "https://fapi.apollox.finance/fapi/v1/exchangeInfo"
