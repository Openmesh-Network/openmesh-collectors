from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Coinbase
from decimal import Decimal
from dateutil import parser
import logging


class CoinbaseStandardiser(Standardiser):
    exchange = Coinbase

    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['size']),
            taker_side=message['side'],
            trade_id=str(message['trade_id']),
            maker_order_id=message['maker_order_id'],
            taker_order_id=message['taker_order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades_l3", **msg)

    async def _open(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['remaining_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _done(self, message):
        if 'price' not in message or not message['price']:
            return
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['remaining_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _change(self, message):
        if 'price' not in message or not message['price']:
            return
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['new_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _ticker(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            ask_price=Decimal(message['best_ask']),
            bid_price=Decimal(message['best_bid']),
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp'],
            ask_size=-1,
            bid_size=-1
        )
        await self.send_to_topic("ticker", **msg)

    async def handle_message(self, msg):
        if 'type' in msg:
            if msg['type'] == 'match' or msg['type'] == 'last_match':
                await self._trade(msg)
            elif msg['type'] == 'open':
                await self._open(msg)
            elif msg['type'] == 'done':
                await self._done(msg)
            elif msg['type'] == 'change':
                await self._change(msg)
            elif msg['type'] == 'batch_ticker' or msg['type'] == 'ticker':
                await self._ticker(msg)
            elif msg['type'] == 'received':
                pass
            elif msg['type'] == 'activate':
                pass
            elif msg['type'] == 'subscriptions':
                pass
            else:
                logging.warning(f"{self.id}: Unhandled message: {msg}")
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
