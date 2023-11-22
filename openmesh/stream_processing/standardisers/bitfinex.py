import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Bitfinex
from decimal import Decimal


class BitfinexStandardiser(Standardiser):
    exchange = Bitfinex

    async def _trade(self, message):
        if message[1] in ('tu', 'hb') or isinstance(message[1], list):
            return

        symbol = self.normalise_symbol(message[-2])
        atom_timestamp = message[-1]
        if isinstance(message[2][0], int):
            message[2] = [message[2]]
        for t in message[2]:
            trade_id, event_timestamp, size, price = t
            msg = dict(
                symbol=symbol,
                price=Decimal(str(price)),
                size=Decimal(str(abs(size))),
                taker_side='buy' if size > 0 else 'sell',
                trade_id=str(trade_id),
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp
            )
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        if message[1] == 'hb':
            return
        symbol = self.normalise_symbol(message[-2])
        event_timestamp = message[2]
        atom_timestamp = message[-1]
        if isinstance(message[1][0], int):
            message[1] = [message[1]]
        for o in message[1]:
            order_id, price, size = o
            msg = dict(
                symbol=symbol,
                order_id=str(order_id),
                price=Decimal(str(price)),
                size=Decimal(str(abs(size))),
                side='buy' if size > 0 else 'sell',
                event_timestamp=event_timestamp,
                atom_timestamp=atom_timestamp
            )
            await self.send_to_topic("lob_l3", **msg)

    async def _ticker(self, message):
        if message[1] == 'hb':
            return

        best_bid, best_bid_size, best_ask, best_ask_size, _, _, _, _, _, _ = message[1]
        symbol = self.normalise_symbol(message[-2])
        event_timestamp = message[2]
        atom_timestamp = message[-1]

        msg = dict(
            symbol=symbol,
            bid_price=Decimal(str(best_bid)),
            bid_size=Decimal(str(best_bid_size)),
            ask_price=Decimal(str(best_ask)),
            ask_size=Decimal(str(best_ask_size)),
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        )
        await self.send_to_topic("ticker", **msg)

    async def _candle(self, message):
        if message[1] == 'hb':
            return
        end, o, c, h, l, v = message[1]
        symbol = self.normalise_symbol(message[-2])
        atom_timestamp = message[-1]
        event_timestamp = atom_timestamp // 1000
        start = end - 60 * 1000
        msg = dict(
            symbol=symbol,
            start=start,
            end=end,
            interval='1m',
            trades=-1,
            closed=event_timestamp >= end,
            o=Decimal(str(o)),
            c=Decimal(str(c)),
            h=Decimal(str(h)),
            l=Decimal(str(l)),
            v=Decimal(str(v)),
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        )
        await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        if msg[-3] == 'trades':
            await self._trade(msg)
        elif msg[-3] == 'lob_l3':
            await self._book(msg)
        elif msg[-3] == 'ticker':
            await self._ticker(msg)
        elif msg[-3] == 'candle':
            await self._candle(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
