import pytest
from unittest.mock import AsyncMock
import json
from decimal import Decimal


@pytest.mark.asyncio()
async def test_binance_futures_agent(mock_kafka, test_app):
    async with test_app.agents['binance-futures_agent'].test_context() as agent:
        topics = agent.fun.__self__.normalised_topics
        for topic in topics.values():
            topic.send = AsyncMock()
        data = json.load(open('mock_data/binance_futures.json'))
        for msg in data:
            await agent.put(msg)
            if 'openInterest' in msg:
                _, kwargs = topics['open_interest'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance-futures'
                assert record['symbol'] == 'BTC.USDT-PERP'
                assert record['open_interest'] == Decimal('145493.197')
            elif msg['e'] == 'markPriceUpdate':
                _, kwargs = topics['funding_rate'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance-futures'
                assert record['symbol'] == 'ETH.USDT-PERP'
                assert record['mark_price'] == Decimal('1323.72000000')
                assert record['funding_rate'] == Decimal('-0.00000304')
                assert record['next_funding_time'] == 1665388800000
