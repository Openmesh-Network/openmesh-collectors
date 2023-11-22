import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Deribit
from decimal import Decimal


class DeribitStandardiser(Standardiser):
    exchange = Deribit

    async def _trade(self, message):
        data = message['params']['data']
        msg = dict(
            symbol=self.normalise_symbol(data[0]['instrument_name']),
            atom_timestamp=message['atom_timestamp']
        )
        for trade in data:
            msg['price'] = Decimal(str(trade['price']))
            msg['size'] = Decimal(str(trade['amount']))
            msg['taker_side'] = trade['direction']
            msg['event_timestamp'] = trade['timestamp']
            msg['trade_id'] = trade['trade_id']
            await self.send_to_topic("trades", **msg)

    async def _book(self, message):
        data = message['params']['data']
        symbol = self.normalise_symbol(data['instrument_name'])
        event_timestamp = data['timestamp']
        atom_timestamp = message['atom_timestamp']
        for s, side in (('bids', 'buy'), ('asks', 'sell')):
            for _, price, size in data[s]:
                msg = dict(
                    symbol=symbol,
                    price=Decimal(str(price)),
                    size=Decimal(size),
                    side=side,
                    event_timestamp=event_timestamp,
                    atom_timestamp=atom_timestamp
                )
                await self.send_to_topic("lob", **msg)

    async def _ticker(self, message):
        data = message['params']['data']
        msg = dict(
            symbol=self.normalise_symbol(data['instrument_name']),
            event_timestamp=data['timestamp'],
            atom_timestamp=message['atom_timestamp']
        )

        ticker_msg = {
            **msg,
            'bid_price': Decimal(str(data['best_bid_price'])),
            'bid_size': Decimal(str(data['best_bid_amount'])),
            'ask_price': Decimal(str(data['best_ask_price'])),
            'ask_size': Decimal(str(data['best_ask_amount'])),
        }
        await self.send_to_topic("ticker", **ticker_msg)

        funding_msg = {
            **msg,
            'funding_rate': Decimal(str(data['funding_8h'])),
            'next_funding_time': None,
            'mark_price': Decimal(str(data['mark_price']))
        }
        await self.send_to_topic("funding_rate", **funding_msg)

        oi_msg = {
            **msg,
            'open_interest': Decimal(str(data['open_interest']))
        }
        await self.send_to_topic("open_interest", **oi_msg)

    async def _candle(self, message):
        data = message['params']['data']
        symbol = self.normalise_symbol(message['params']['channel'].split('.')[2])
        atom_timestamp = message['atom_timestamp']
        event_timestamp = data['tick']
        msg = dict(
            symbol=symbol,
            start=event_timestamp - 60 * 1000,
            end=event_timestamp,
            interval='1m',
            trades=-1,
            closed=True,
            o=Decimal(str(data['open'])),
            h=Decimal(str(data['high'])),
            l=Decimal(str(data['low'])),
            c=Decimal(str(data['close'])),
            v=Decimal(str(data['volume'])),
            event_timestamp=event_timestamp,
            atom_timestamp=atom_timestamp
        )
        await self.send_to_topic("candle", **msg)

    async def handle_message(self, msg):
        if 'params' in msg:
            data = msg['params']
            channel = data['channel'].split('.')[0]
            if channel == 'ticker':
                await self._ticker(msg)
            elif channel == 'chart':
                await self._candle(msg)
            elif channel == 'trades':
                await self._trade(msg)
            elif channel == 'book':
                await self._book(msg)
            else:
                logging.warning(f'Unknown channel {channel}')
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
