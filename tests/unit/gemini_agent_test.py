import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import GeminiStandardiser


@pytest.mark.asyncio()
async def test_gemini_agent(mock_kafka):
    exchange = GeminiStandardiser(['BTC.USD', 'ETH.USD'])
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/gemini.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if msg['type'] == 'l2_updates':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'gemini'
            assert record['symbol'] == 'ETH.USD'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('1320.77')
        if msg['type'] == 'trade':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'gemini'
            assert record['symbol'] == 'ETH.USD'
            assert record['taker_side'] == 'buy'
            assert record['price'] == Decimal('1321.56')
            assert record['size'] == Decimal('0.011293')
            assert record['trade_id'] == '147298363555'
        if msg['type'] == 'candles_1m_updates':
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'gemini'
            assert record['symbol'] == 'ETH.USD'
            assert record['o'] == Decimal('1320.65')
            assert record['h'] == Decimal('1321.67')
            assert record['v'] == Decimal('4.004986')
