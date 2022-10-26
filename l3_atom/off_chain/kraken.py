from l3_atom.orderbook_exchange import OrderBookExchangeFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json
from l3_atom.helpers.enrich_data import enrich_raw

class Kraken(OrderBookExchangeFeed):
    name = "kraken"
    key_field = -2
    ws_endpoints = {
        WSEndpoint("wss://ws.kraken.com"): ["lob", "ticker", 'trades', 'candle']
    }

    ws_channels = {
        "lob": "book",
        "trades": "trade",
        "candle": "ohlc",
        "ticker": "ticker"
    }

    symbols_endpoint = "https://api.kraken.com/0/public/AssetPairs"

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        res = sym_list['result']

        for s in res:
            sym = res[s]
            name = sym.get('wsname', None)
            if name is None:
                return
            rep_name = name.replace('XBT', 'BTC').replace('XDG', 'DOGE')
            base, quote = rep_name.split('/')
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = name
        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        for feed in feeds:
            channel = self.get_channel_from_feed(feed)
            subscription = {"name": channel}
            if feed == 'lob':
                subscription['depth'] = 1000
            elif feed == 'candle':
                subscription['interval'] = 1
            msg = json.dumps({
                "event": "subscribe",
                "pair": symbols,
                "subscription": subscription
            })
            await conn.send_data(msg)

    def auth(self, conn: WSConnection):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        """
        First method called when a message is received from the exchange. Currently forwards the message to Kafka to be produced.

        :param message: Message received from the exchange
        :type message: str
        :param conn: Connection the message was received from
        :type conn: AsyncFeed
        :param channel: Channel the message was received on
        :type channel: str
        """
        msg = json.loads(message)
        msg = enrich_raw(msg, timestamp)
        try:
            if msg[-3].startswith('book'):
                if len(msg[1]["b"]) >= 1 and len(msg[1]["b"][0]) != 3:
                    print(json.dumps(msg))
        except:
            pass
        await self.kafka_connector.write(json.dumps(msg))
