from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json
from l3_atom.helpers.enrich_data import enrich_raw

class Okx(OrderBookExchangeFeed):
    name = "okx"
    key_field = -2 //nfi
    ws_endpoints = {
        WSEndpoint("wss://ws.okx.com:8443/ws/v5/public"): ["lob", "ticker", 'trades', 'candle']
    }

    ws_channels = {
        "lob": "book",
        "trades": 'trades',
        "ticker": "ticker",
        "candle": "candles"
    }

    symbols_endpoint = [f'https://www.okx.com/api/v5/market/tickers?instType={instruments}' for instruments in [ 'SPOT]
