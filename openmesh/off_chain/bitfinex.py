from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json
from openmesh.helpers.enrich_data import enrich_raw

# Configuration options for Bitfinex API
TIMESTAMP = 32768
SEQ_ALL = 65536
OB_CHECKSUM = 131072
BULK_UPDATES = 536870912


class Bitfinex(DataFeed):
    name = "bitfinex"

    # Bitfinex sends arrays, so we provide an index instead of a key
    sym_field = -2
    type_field = -3

    ws_endpoints = {
        WSEndpoint("wss://api.bitfinex.com/ws/2"): ["ticker", "candle", "trades"]
    }

    ws_channels = {
        "lob_l3": "book",
        "trades": 'trades',
        "ticker": "ticker",
        "candle": "candles"
    }

    symbols_endpoint = ["https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange",
                        "https://api-pub.bitfinex.com/v2/conf/pub:list:currency", "https://api-pub.bitfinex.com/v2/conf/pub:list:pair:futures"]

    def __init__(self, symbols=None):
        super().__init__(symbols)
        self.chan_ids = dict()

    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}

        currency_pairs = sym_list[0][0]
        currencies = sym_list[1][0]
        perpetual_futures = sym_list[2][0]

        for p in currency_pairs:
            p_norm = p.replace('UST', 'USDT')
            if ':' in p_norm:
                p_norm = p_norm.split(':')
            else:
                p_norm = [p_norm[:3], p_norm[3:]]
            base, quote = p_norm
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = f't{p}'

        for c in currencies:
            c_norm = c.replace('UST', 'USDT')
            normalised_symbol = Symbol(c_norm, c_norm)
            ret[normalised_symbol] = f'f{c}'

        for f in perpetual_futures:
            f_norm = f.replace('UST', 'USDT')
            base, quote = (i[:-2] for i in f_norm.split(':'))
            normalised_symbol = Symbol(base, quote, symbol_type='perpetual')
            ret[normalised_symbol] = f't{f}'

        return ret

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        await conn.send_data(json.dumps({
            'event': 'conf',
            'flags': TIMESTAMP ^ BULK_UPDATES
        }))
        for feed in feeds:
            for symbol in symbols:
                msg = {
                    "event": "subscribe",
                    "channel": self.get_channel_from_feed(feed),
                    "symbol": symbol
                }

                if feed == 'lob_l3':
                    msg['prec'] = 'R0'
                    msg['freq'] = 'F0'
                    msg['len'] = 100

                if feed == 'candle':
                    msg['key'] = f'trade:1m:{symbol}'
                    del msg['symbol']
                await conn.send_data(json.dumps(msg))
                res = json.loads(await conn.conn.recv())
                while 'chanId' not in res:
                    res = json.loads(await conn.conn.recv())
                self.chan_ids[res['chanId']] = (feed, symbol)

    def auth(self, conn: WSConnection):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        """
        First method called when a message is received from the exchange. Overloaded as Bitfinex stores unique channel IDs for each subscription that we need to keep track of.

        :param message: Message received from the exchange
        :type message: str
        :param conn: Connection the message was received from
        :type conn: AsyncFeed
        :param channel: Channel the message was received on
        :type channel: str
        """
        msg = json.loads(message)
        chan_id = msg[0]
        if isinstance(chan_id, int):
            channel, symbol = self.chan_ids[chan_id]
            msg.append(channel)
            msg.append(symbol)
        msg = enrich_raw(msg, timestamp)
        await self.kafka_connector.write(json.dumps(msg))
