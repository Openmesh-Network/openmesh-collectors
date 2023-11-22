import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Dydx
from decimal import Decimal
from dateutil import parser


class DydxStandardiser(Standardiser):
    exchange = Dydx

    def start_exchange(self):
        super().start_exchange()
        self.book_sequences = {
            sym: {} for sym in self.exchange.symbols
        }

    async def _trade(self, message):
        if message['type'] != 'channel_data':
            return
        msg = dict(
            symbol=self.normalise_symbol(message['id']),
            atom_timestamp=message['atom_timestamp'],
            trade_id=""
        )
        for trade in message['contents']['trades']:
            msg['price'] = Decimal(trade['price'])
            msg['size'] = Decimal(trade['size'])
            msg['taker_side'] = trade['side'].lower()
            msg['event_timestamp'] = int(parser.isoparse(
                trade['createdAt']).timestamp() * 1000)
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        # Snapshot
        if message['type'] != 'channel_data':
            return
        symbol = self.normalise_symbol(message['id'])
        # Have to track offsets since Dydx doesn't guarantee order
        offset = int(message['contents']['offset'])
        atom_timestamp = message['atom_timestamp']
        # Event timestamps not provided in order book. Next best thing is the atom timestamp in milliseconds
        event_timestamp = atom_timestamp // 1000
        for s, side in (('bids', 'buy'), ('asks', 'sell')):
            for price, size in message['contents'][s]:
                if self.book_sequences[symbol].get(price, 0) > offset:
                    continue
                msg = dict(
                    symbol=symbol,
                    price=Decimal(price),
                    size=Decimal(size),
                    side=side,
                    event_timestamp=event_timestamp,
                    atom_timestamp=atom_timestamp
                )
                self.book_sequences[symbol][price] = offset
                await self.send_to_topic("lob", **msg)

    async def handle_message(self, msg):
        if 'channel' in msg:
            if msg['channel'] == 'v3_trades':
                await self._trade(msg)
            elif msg['channel'] == 'v3_orderbook':
                await self._book(msg)
            else:
                logging.warning(f"{self.id}: Unhandled message: {msg}")
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
