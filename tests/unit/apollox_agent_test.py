import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import ApolloXStandardiser


@pytest.mark.asyncio()
async def test_binance_futures_agent(mock_kafka):
    exchange = ApolloXStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/apollox.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if 'openInterest' in msg:
            _, kwargs = exchange.normalised_topics['open_interest'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'apollox'
            assert record['symbol'] == 'BTC.USDT-PERP'
            assert record['open_interest'] == Decimal('145493.197')
        elif msg['e'] == 'markPriceUpdate':
            _, kwargs = exchange.normalised_topics['funding_rate'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'apollox'
            assert record['symbol'] == 'ETH.USDT-PERP'
            assert record['mark_price'] == Decimal('1323.72000000')
            assert record['funding_rate'] == Decimal('-0.00000304')
            assert record['next_funding_time'] == 1665388800000
