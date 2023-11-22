import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import BinanceStandardiser


@pytest.mark.asyncio()
async def test_binance_agent(mock_kafka):
    exchange = BinanceStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/binance.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if 'A' in msg:
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance'
            assert record['symbol'] == 'BTC.USDT'
            assert record['bid_price'] == Decimal('0.0432345')
            assert record['ask_price'] == Decimal('0.765000')
            assert record['bid_size'] == Decimal('50.00000000')
            assert record['ask_size'] == Decimal('32.40000000')
        elif msg['e'] == 'trade':
            _, kwargs = exchange.normalised_topics['trades_l3'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance'
            assert record['symbol'] == 'ETH.USDT'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('0.00861')
            assert record['size'] == Decimal('4')
            assert record['taker_order_id'] == '123456785'
            assert record['trade_id'] == '3543'
        elif msg['e'] == 'depthUpdate':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance'
            assert record['symbol'] == 'ETH.USDT'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('1320.06000000')
            assert record['size'] == Decimal('11.62390000')
        elif msg['e'] == 'kline':
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance'
            assert record['symbol'] == 'BTC.USDT'
            assert record['o'] == Decimal('19453.47000000')
            assert record['h'] == Decimal('19453.47000000')
            assert record['l'] == Decimal('19453.03000000')
            assert record['closed'] is True
            assert record['trades'] == 7
