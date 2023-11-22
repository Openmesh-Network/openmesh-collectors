import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import PhemexStandardiser


@pytest.mark.asyncio()
async def test_phemex_agent(mock_kafka):
    exchange = PhemexStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/phemex.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if 'book' in msg:
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'phemex'
            assert record['symbol'] == 'BTC.USDT'
            assert record['side'] == 'sell'
            assert record['price'] == Decimal('19426.46')
        elif 'trades' in msg:
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'phemex'
            assert record['symbol'] == 'BTC.USDT'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19419.7')
            assert record['size'] == Decimal('0.01454')
            assert record['trade_id'] == ''
        elif 'kline' in msg:
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'phemex'
            assert record['symbol'] == 'BTC.USDT'
            assert record['o'] == Decimal('19419.77')
            assert record['h'] == Decimal('19419.77')
            assert record['v'] == Decimal('0.159411')
