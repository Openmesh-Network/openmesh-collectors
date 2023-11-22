import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Phemex
from decimal import Decimal


class PhemexStandardiser(Standardiser):
    exchange = Phemex

    def _get_price_scale(self, sym):
        return self.exchange.price_decimal_places[sym]

    def _get_size_scale(self, sym):
        return self.exchange.qty_decimal_places[sym]

    async def _trade(self, message):
        if message['type'] == 'snapshot':
            return
        symbol = self.normalise_symbol(message['symbol'])
        atom_timestamp = message['atom_timestamp']
        for event_timestamp, side, price, size in message['trades']:
            msg = dict(
                symbol=symbol,
                price=Decimal(price) / Decimal(self._get_price_scale(symbol)),
                size=Decimal(size) / Decimal(self._get_size_scale(symbol)),
                taker_side=side.lower(),
                event_timestamp=event_timestamp // 1_000_000,
                atom_timestamp=atom_timestamp,
                trade_id=""
            )
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        if message['type'] == 'snapshot':
            return
        symbol = self.normalise_symbol(message['symbol'])
        event_timestamp = message['timestamp'] // 1_000_000
        atom_timestamp = message['atom_timestamp']
        for s, side in (('bids', 'buy'), ('asks', 'sell')):
            for price, size in message['book'][s]:
                msg = dict(
                    symbol=symbol,
                    price=Decimal(price) /
                    Decimal(self._get_price_scale(symbol)),
                    size=Decimal(size) / Decimal(self._get_size_scale(symbol)),
                    side=side,
                    event_timestamp=event_timestamp,
                    atom_timestamp=atom_timestamp
                )
                await self.send_to_topic("lob", **msg)

    async def _candle(self, message):
        if message['type'] == 'snapshot':
            return
        interval = '1m'
        atom_timestamp = message['atom_timestamp']
        symbol = self.normalise_symbol(message['symbol'])
        price_scale = Decimal(self._get_price_scale(symbol))
        for event_timestamp, _, _, o, h, l, c, v, _ in message['kline']:
            op = Decimal(o) / price_scale
            high = Decimal(h) / price_scale
            low = Decimal(l) / price_scale
            close = Decimal(c) / price_scale
            vol = Decimal(v) / Decimal(self._get_size_scale(symbol))
            msg = dict(
                symbol=symbol,
                start=event_timestamp - 60_000,
                end=event_timestamp,
                interval=interval,
                trades=-1,
                closed=True,
                o=op,
                h=high,
                l=low,
                c=close,
                v=vol,
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp
            )
            await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        if 'trades' in msg:
            await self._trade(msg)
        elif 'book' in msg:
            await self._book(msg)
        elif 'kline' in msg:
            await self._candle(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
