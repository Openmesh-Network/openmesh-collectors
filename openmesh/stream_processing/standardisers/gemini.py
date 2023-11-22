import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Gemini
from decimal import Decimal


class GeminiStandardiser(Standardiser):
    exchange = Gemini

    def __init__(self, symbols: list = None):
        super().__init__()
        self.symbols = symbols
        self.sym_map = {}

    def normalise_symbol(self, symbol):
        if symbol not in self.sym_map:
            temp_map = self.exchange.normalise_symbols(
                self.exchange.get_symbols([symbol]))
            norm, _ = list(temp_map.items())[0]
            self.sym_map[symbol] = norm
        return self.sym_map[symbol].normalised

    def start_exchange(self):
        self.exchange = self.exchange(symbols=self.symbols)
        self.exchange_started = True

    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['symbol']),
            price=Decimal(message['price']),
            size=Decimal(message['quantity']),
            taker_side=message['side'],
            trade_id=str(message['event_id']),
            event_timestamp=message['timestamp'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        symbol = self.normalise_symbol(message['symbol'])
        atom_timestamp = message['atom_timestamp']
        event_timestamp = atom_timestamp // 1000
        for event in message['changes']:
            side, price, size = event
            msg = dict(
                symbol=symbol,
                price=Decimal(price),
                size=Decimal(size),
                side=side,
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp
            )
            await self.send_to_topic("lob", **msg)

    async def _candle(self, message):
        symbol = self.normalise_symbol(message['symbol'])
        atom_timestamp = message['atom_timestamp']
        for m in message['changes']:
            event_timestamp, o, h, l, c, v = m
            end = event_timestamp
            start = end - 60 * 1000
            msg = dict(
                symbol=symbol,
                start=start,
                end=end,
                o=Decimal(str(o)),
                h=Decimal(str(h)),
                l=Decimal(str(l)),
                c=Decimal(str(c)),
                v=Decimal(str(v)),
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp,
                closed=True,
                interval='1m',
                trades=-1
            )
            await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        # Snapshot
        if "trades" in msg:
            return
        elif msg['type'] == 'trade':
            await self._trade(msg)
        elif msg['type'] == 'l2_updates':
            await self._book(msg)
        elif msg['type'] == 'candles_1m_updates':
            await self._candle(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
