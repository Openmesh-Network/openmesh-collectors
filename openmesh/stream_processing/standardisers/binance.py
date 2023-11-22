import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Binance
from decimal import Decimal


class BinanceStandardiser(Standardiser):
    exchange = Binance

    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['s']),
            price=Decimal(message['p']),
            size=Decimal(message['q']),
            taker_side='sell' if message['m'] else 'buy',
            trade_id=str(message['t']),
            maker_order_id=str(
                message['b']) if message['m'] else str(message['a']),
            taker_order_id=str(
                message['a']) if message['m'] else str(message['b']),
            event_timestamp=message['E'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades_l3", **msg)

    async def _book(self, message):
        symbol = self.normalise_symbol(message['s'])
        event_timestamp = message['E']
        atom_timestamp = message['atom_timestamp']
        for s, side in (('b', 'buy'), ('a', 'sell')):
            for price, size in message[s]:
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
        msg = dict(
            symbol=self.normalise_symbol(message['s']),
            ask_price=Decimal(message['a']),
            bid_price=Decimal(message['b']),
            ask_size=Decimal(message['A']),
            bid_size=Decimal(message['B']),
            event_timestamp=message['E'] if 'E' in message else message['atom_timestamp'] // 1000,
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("ticker", **msg)

    async def _candle(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['s']),
            start=message['k']['t'],
            end=message['k']['T'],
            interval=message['k']['i'],
            trades=message['k']['n'],
            closed=message['k']['x'],
            o=Decimal(message['k']['o']),
            h=Decimal(message['k']['h']),
            l=Decimal(message['k']['l']),
            c=Decimal(message['k']['c']),
            v=Decimal(message['k']['v']),
            event_timestamp=message['E'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        if 'e' in msg:
            if msg['e'] == 'trade':
                await self._trade(msg)
            elif msg['e'] == 'depthUpdate':
                await self._book(msg)
            elif msg['e'] == 'kline':
                await self._candle(msg)
            elif msg['e'] == 'bookTicker':
                await self._ticker(msg)
        elif 'A' in msg:
            await self._ticker(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
