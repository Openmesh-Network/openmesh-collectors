from .binance import BinanceStandardiser
from openmesh.off_chain import BinanceFutures
from decimal import Decimal
import logging


class BinanceFuturesStandardiser(BinanceStandardiser):
    exchange = BinanceFutures

    async def _funding_rate(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['s']),
            mark_price=Decimal(message['p']),
            funding_rate=Decimal(message['r']),
            next_funding_time=message['T'],
            predicted_rate=-1,
            event_timestamp=message['E'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("funding_rate", **msg)

    async def _open_interest(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['symbol']),
            open_interest=Decimal(message['openInterest']),
            event_timestamp=message['time'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("open_interest", **msg)

    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['s']),
            price=Decimal(message['p']),
            size=Decimal(message['q']),
            taker_side=message['m'] and 'buy' or 'sell',
            trade_id=str(message['a']),
            event_timestamp=message['E'],
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades", **msg)

    async def handle_message(self, msg):
        if 'openInterest' in msg:
            await self._open_interest(msg)
        elif 'e' in msg:
            if msg['e'] == 'aggTrade':
                await self._trade(msg)
            elif msg['e'] == 'depthUpdate':
                await self._book(msg)
            elif msg['e'] == 'kline':
                await self._candle(msg)
            elif msg['e'] == 'markPriceUpdate':
                await self._funding_rate(msg)
            elif msg['e'] == 'bookTicker':
                await self._ticker(msg)
        elif 'A' in msg:
            await self._ticker(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
