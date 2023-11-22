import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Kraken
from decimal import Decimal


class KrakenStandardiser(Standardiser):
    exchange = Kraken

    async def _trade(self, message):
        symbol = self.normalise_symbol(message[-2])
        atom_timestamp = message[-1]
        for t in message[1]:
            price, size, event_timestamp, side, _, _ = t
            event_timestamp = int(Decimal(event_timestamp) * 1000)
            msg = dict(
                symbol=symbol,
                price=Decimal(price),
                size=Decimal(size),
                taker_side='buy' if side == 'b' else 'sell',
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp,
                trade_id=""
            )
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        symbol = self.normalise_symbol(message[-2])
        atom_timestamp = message[-1]
        data = message[1]
        for s, side in (('b', 'buy'), ('a', 'sell')):
            for e in data.get(s, []):
                # Kraken has some messages with an appended "r" for republished messages, which we ignore
                if len(e) == 4:
                    continue
                else:
                    price, size, event_timestamp = e
                event_timestamp = int(Decimal(event_timestamp) * 1000)
                msg = dict(
                    symbol=symbol,
                    price=Decimal(price),
                    size=Decimal(size),
                    side=side,
                    event_timestamp=event_timestamp,
                    atom_timestamp=atom_timestamp
                )
                await self.send_to_topic("lob", **msg)

    async def _ticker(self, message):
        data = message[1]
        atom_timestamp = message[-1]
        msg = dict(
            symbol=self.normalise_symbol(message[-2]),
            ask_price=Decimal(data['a'][0]),
            bid_price=Decimal(data['b'][0]),
            ask_size=Decimal(data['a'][2]),
            bid_size=Decimal(data['b'][2]),
            event_timestamp=atom_timestamp // 1000,
            atom_timestamp=atom_timestamp
        )
        await self.send_to_topic("ticker", **msg)

    async def _candle(self, message):
        start, end, o, h, l, c, _, v, trades = message[1]
        start = int(Decimal(start) * 1000)
        end = int(Decimal(end) * 1000)
        msg = dict(
            symbol=self.normalise_symbol(message[-2]),
            start=start,
            end=end,
            o=Decimal(o),
            h=Decimal(h),
            l=Decimal(l),
            c=Decimal(c),
            v=Decimal(v),
            trades=trades,
            event_timestamp=end,
            atom_timestamp=message[-1],
            interval='1m'
        )
        await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        if isinstance(msg, dict):
            return
        elif msg[-3] == 'trade':
            await self._trade(msg)
        elif msg[-3].startswith('book'):
            await self._book(msg)
        elif msg[-3] == 'ticker':
            await self._ticker(msg)
        elif msg[-3].startswith('ohlc'):
            await self._candle(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
