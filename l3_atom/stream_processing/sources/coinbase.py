from l3_atom.stream_processing.standardiser import Standardiser
from l3_atom.off_chain import Coinbase
from l3_atom.stream_processing.records import TradeL3, LobL3, Ticker
from decimal import Decimal
from dateutil import parser

class CoinbaseStandardiser(Standardiser):
    raw_topic = "coinbase_raw"
    feeds = ['lob_l3', 'trades_l3', 'ticker']
    exchange = Coinbase()

    async def _trade(self, message):
        symbol = self.normalise_symbol(message['product_id'])
        price = Decimal(message['price'])
        size = Decimal(message['size'])
        side = message['side']
        trade_id = str(message['trade_id'])
        maker_order_id = message['maker_order_id']
        taker_order_id = message['taker_order_id']
        event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
        atom_timestamp = message['atom_timestamp']
        await self.normalised_topics["trades_l3"].send(value=TradeL3(
            exchange='coinbase',
            symbol=symbol,
            price=price,
            size=size,
            taker_side=side,
            trade_id=trade_id,
            maker_order_id=maker_order_id,
            taker_order_id=taker_order_id,
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        ), key=symbol)

    async def _open(self, message):
        symbol = self.normalise_symbol(message['product_id'])
        price = Decimal(message['price'])
        size = Decimal(message['remaining_size'])
        side = 'ask' if message['side'] == 'sell' else 'bid'
        order_id = message['order_id']
        event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
        atom_timestamp = message['atom_timestamp']
        await self.normalised_topics["lob_l3"].send(value=LobL3(
            exchange='coinbase',
            symbol=symbol,
            price=price,
            size=size,
            side=side,
            order_id=order_id,
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        ), key=symbol)

    async def _done(self, message):
        if 'price' not in message or not message['price']:
            return
        symbol = self.normalise_symbol(message['product_id'])
        price = Decimal(message['price'])
        size = Decimal(message['remaining_size'])
        side = message['side']
        order_id = message['order_id']
        event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
        atom_timestamp = message['atom_timestamp']
        await self.normalised_topics["lob_l3"].send(value=LobL3(
            exchange='coinbase',
            symbol=symbol,
            price=price,
            size=size,
            side=side,
            order_id=order_id,
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        ), key=symbol)

    async def _change(self, message):
        if 'price' not in message or not message['price']:
            return
        symbol = self.normalise_symbol(message['product_id'])
        price = Decimal(message['price'])
        size = Decimal(message['remaining_size'])
        side = message['side']
        order_id = message['order_id']
        event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
        atom_timestamp = message['atom_timestamp']
        await self.normalised_topics["lob_l3"].send(value=LobL3(
            exchange='coinbase',
            symbol=symbol,
            price=price,
            size=size,
            side=side,
            order_id=order_id,
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        ), key=symbol)

    async def _ticker(self, message):
        symbol = self.normalise_symbol(message['product_id'])
        ask_price = Decimal(message['best_ask'])
        bid_price = Decimal(message['best_bid'])
        event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
        atom_timestamp = message['atom_timestamp']
        await self.normalised_topics["ticker"].send(value=Ticker(
            exchange='coinbase',
            symbol=symbol,
            ask_price=ask_price,
            ask_size=-1,
            bid_price=bid_price,
            bid_size=-1,
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        ), key=symbol)

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