import logging
from l3_atom.stream_processing.standardiser import Standardiser
from l3_atom.off_chain import FTX
from decimal import Decimal
from dateutil import parser


class FTXStandardiser(Standardiser):
    exchange = FTX

    async def _trade(self, message):
        data = message['data']
        symbol = self.normalise_symbol(message['market'])
        atom_timestamp = message['atom_timestamp']

        for t in data:
            msg = dict(
                symbol=symbol,
                price=Decimal(str(t['price'])),
                size=Decimal(str(t['size'])),
                taker_side=t['side'],
                trade_id=str(t['id']),
                event_timestamp=int(parser.isoparse(
                    t['time']).timestamp() * 1000),
                atom_timestamp=atom_timestamp
            )
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        data = message['data']
        symbol = self.normalise_symbol(message['market'])
        event_timestamp = int(data['time'] * 1000)
        atom_timestamp = message['atom_timestamp']
        for s, side in (('bids', 'buy'), ('asks', 'sell')):
            for price, size in data[s]:
                msg = dict(
                    symbol=symbol,
                    price=Decimal(str(price)),
                    size=Decimal(str(size)),
                    side=side,
                    event_timestamp=event_timestamp,
                    atom_timestamp=atom_timestamp
                )
                await self.send_to_topic("lob", **msg)

    async def _ticker(self, message):
        data = message['data']
        msg = dict(
            symbol=self.normalise_symbol(message['market']),
            ask_price=Decimal(str(data['ask'])),
            bid_price=Decimal(str(data['bid'])),
            ask_size=Decimal(str(data['askSize'])),
            bid_size=Decimal(str(data['bidSize'])),
            event_timestamp=int(data['time'] * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("ticker", **msg)

    async def handle_message(self, msg):
        if 'channel' in msg and msg.get('type') != 'subscribed':
            if msg['channel'] == 'trades':
                await self._trade(msg)
            elif msg['channel'] == 'orderbook':
                await self._book(msg)
            elif msg['channel'] == 'ticker':
                await self._ticker(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
