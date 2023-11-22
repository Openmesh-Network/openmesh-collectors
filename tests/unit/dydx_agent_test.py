import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import DydxStandardiser


@pytest.mark.asyncio()
async def test_dydx_agent(mock_kafka):
    exchange = DydxStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/dydx.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if msg['channel'] == 'v3_orderbook':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'dydx'
            assert record['symbol'] == 'ETH.USD-PERP'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('1288.5')
            assert record['size'] == Decimal('55.192')
            assert record['event_timestamp'] == 503485904
        elif msg['channel'] == 'v3_trades':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'dydx'
            assert record['symbol'] == 'BTC.USD-PERP'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19088')
            assert record['size'] == Decimal('0.0224')
            assert record['trade_id'] == ""
            assert record['event_timestamp'] == 1665641065258
