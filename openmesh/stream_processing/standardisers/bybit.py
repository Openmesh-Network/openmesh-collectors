import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Bybit
from decimal import Decimal


class BybitStandardiser(Standardiser):
    exchange = Bybit

    async def _trade(self, message):
        data = message['data']
        msg = dict(
            symbol=self.normalise_symbol(message['topic'].split('.')[1]),
            price=Decimal(data['p']),
            size=Decimal(data['q']),
            taker_side='buy' if data['m'] else 'sell',
            trade_id=str(data['v']),
            event_timestamp=data['t'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        data = message['data']
        symbol = self.normalise_symbol(data['s'])
        event_timestamp = data['t']
        atom_timestamp = message['atom_timestamp']
        for s, side in (('b', 'buy'), ('a', 'sell')):
            for price, size in data[s]:
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
        data = message['data']
        msg = dict(
            symbol=self.normalise_symbol(data['s']),
            ask_price=Decimal(data['ap']),
            bid_price=Decimal(data['bp']),
            ask_size=Decimal(data['aq']),
            bid_size=Decimal(data['bq']),
            event_timestamp=data['t'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("ticker", **msg)

    async def _candle(self, message):
        data = message['data']
        event_timestamp = message['ts']
        start = data['t']
        end = start + 60 * 1000
        msg = dict(
            symbol=self.normalise_symbol(data['s']),
            start=start,
            end=end,
            interval=message['topic'].split('.')[1],
            trades=-1,
            closed=event_timestamp >= end,
            o=Decimal(data['o']),
            h=Decimal(data['h']),
            l=Decimal(data['l']),
            c=Decimal(data['c']),
            v=Decimal(data['v']),
            event_timestamp=event_timestamp,
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        topic = msg['topic']
        if topic.startswith('trade'):
            await self._trade(msg)
        elif topic.startswith('orderbook'):
            await self._book(msg)
        elif topic.startswith('kline'):
            await self._candle(msg)
        elif topic.startswith('bookticker'):
            await self._ticker(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
