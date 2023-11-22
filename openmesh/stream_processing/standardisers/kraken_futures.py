import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import KrakenFutures
from decimal import Decimal


class KrakenFuturesStandardiser(Standardiser):
    exchange = KrakenFutures

    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message["product_id"]),
            taker_side=message['side'],
            size=Decimal(str(message['qty'])),
            price=Decimal(str(message["price"])),
            event_timestamp=message["time"],
            atom_timestamp=message["atom_timestamp"],
            trade_id=message['uid']
        )
        await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            event_timestamp=message['timestamp'],
            atom_timestamp=message["atom_timestamp"],
            price=Decimal(str(message['price'])),
            size=Decimal(str(message["qty"])),
            side=message['side']
        )
        await self.send_to_topic("lob", **msg)

    async def _ticker(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message["product_id"]),
            event_timestamp=message["time"],
            atom_timestamp=message["atom_timestamp"],
        )

        ticker_msg = {
            **msg,
            "bid_price": Decimal(str(message["bid"])),
            "bid_size": Decimal(str(message["bid_size"])),
            "ask_price": Decimal(str(message["ask"])),
            "ask_size": Decimal(str(message["ask_size"])),
        }
        await self.send_to_topic("ticker", **ticker_msg)

        funding_msg = {
            **msg,
            "funding_rate": Decimal(str(message["funding_rate"])),
            "next_funding_time": message['next_funding_rate_time'],
            "mark_price": Decimal(str(message["markPrice"])),
            'predicted_rate': Decimal(str(message['funding_rate_prediction']))
        }
        await self.send_to_topic("funding_rate", **funding_msg)

        oi_msg = {
            **msg, "open_interest": Decimal(str(message["openInterest"]))}
        await self.send_to_topic("open_interest", **oi_msg)

    async def handle_message(self, msg):
        if msg['feed'] == 'ticker':
            await self._ticker(msg)
        elif msg['feed'] == 'trade':
            await self._trade(msg)
        elif msg['feed'] == 'book':
            await self._book(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
