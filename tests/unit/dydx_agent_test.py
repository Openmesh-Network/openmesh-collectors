import pytest
from unittest.mock import AsyncMock
import json
from decimal import Decimal


@pytest.mark.asyncio()
async def test_dydx_agent(mock_kafka, test_app):
    async with test_app.agents['dydx_agent'].test_context() as agent:
        topics = agent.fun.__self__.normalised_topics
        for topic in topics.values():
            topic.send = AsyncMock()
        data = json.load(open('mock_data/dydx.json'))
        for msg in data:
            await agent.put(msg)
            if msg['channel'] == 'v3_orderbook':
                _, kwargs = topics['lob'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'dydx'
                assert record['symbol'] == 'ETH.USD-PERP'
                assert record['side'] == 'buy'
                assert record['price'] == Decimal('1288.5')
                assert record['size'] == Decimal('55.192')
                assert record['event_timestamp'] == 503485904
            elif msg['channel'] == 'v3_trades':
                _, kwargs = topics['trades'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'dydx'
                assert record['symbol'] == 'BTC.USD-PERP'
                assert record['taker_side'] == 'sell'
                assert record['price'] == Decimal('19088')
                assert record['size'] == Decimal('0.0224')
                assert record['trade_id'] == ""
                assert record['event_timestamp'] == 1665641065258
