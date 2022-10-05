from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json

class Binance(OrderBookExchangeFeed):
    name = "binance"
    key_field = 's'
    ws_endpoints = {
        WSEndpoint("wss://stream.binance.com:9443/ws"): ["lob", "ticker", "candle", "trades"]
    }

    ws_channels = {
        "lob": "depth@100ms",
        "trades": 'trade',
        "ticker": "bookTicker",
        "candle": "kline_1s"
    }

    symbols_endpoint = "https://api.binance.com/api/v3/exchangeInfo"
        
    def normalize_symbols(self, sym_list: list) -> dict:
        ret = {}
        for m in sym_list['symbols']:
            base, quote = m['baseAsset'], m['quoteAsset']
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = m['symbol']
        return ret
    
    async def subscribe(self, conn: AsyncFeed, channels: list, symbols):
        for channel in channels:
            msg = json.dumps({
                "method": "SUBSCRIBE",
                "params": [
                    f"{symbol.lower()}@{self.get_feed_from_channel(channel)}"
                    for symbol in symbols
                ],
                "id": 1
            })
            await conn.send_data(msg)
            print(msg)

    def auth(self, conn: WSConnection):
        pass