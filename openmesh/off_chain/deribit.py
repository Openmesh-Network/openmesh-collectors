from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Deribit(DataFeed):
    name = "deribit"
    ws_endpoints = {
        WSEndpoint("wss://www.deribit.com/ws/api/v2"): ["ticker", "trades", "candle"]
    }

    ws_channels = {
        "lob": "book",
        "trades": 'trades',
        "ticker": "ticker",
        "funding_rate": "ticker",
        "open_interest": "ticker",
        "candle": "chart.trades"
    }

    symbols_endpoint = [
        f'https://www.deribit.com/api/v2/public/get_instruments?currency={sym}' for sym in ['BTC', 'ETH', 'USDT', 'USDC']]

    @classmethod
    def get_key(cls, msg):
        if 'params' in msg:
            return f'{cls.name}_{msg["params"]["channel"]}'.encode()

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for currency in sym_list:
            for t in currency['result']:
                base, quote = t['base_currency'], t['quote_currency']
                symbol_type = 'perpetual' if t['settlement_period'] == 'perpetual' else t['kind']
                if symbol_type in ('future_combo', 'option_combo'):
                    continue
                if symbol_type == 'future':
                    symbol_type = 'futures'
                option_type = t.get('option_type', None)
                strike_price = None
                if 'strike' in t:
                    strike_price = int(t['strike'])
                expiry = t['expiration_timestamp']
                normalised_symbol = Symbol(base, quote, symbol_type=symbol_type,
                                           option_type=option_type, strike_price=strike_price, expiry_date=expiry // 1000)
                ret[normalised_symbol] = t['instrument_name']
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {
                "channels": []
            }
        }

        for symbol in symbols:
            chans = []
            for feed in feeds:
                channel = self.get_channel_from_feed(feed)
                if feed == 'candle':
                    chans.append(f"{channel}.{symbol}.1")
                else:
                    # TODO: Set up authentication to allow for raw feeds
                    chans.append(f"{channel}.{symbol}.100ms")
            msg['params']['channels'] = chans
            await conn.send_data(json.dumps(msg))

    def auth(self, conn: WSConnection):
        pass
