import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from l3_atom.stream_processing.standardisers import FTXStandardiser


@pytest.mark.asyncio()
async def test_ftx_agent(mock_kafka):
    exchange = FTXStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/ftx.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if msg['channel'] == 'ticker':
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'ftx'
            assert record['symbol'] == 'ETH.USD'
            assert record['bid_price'] == Decimal('1303')
            assert record['ask_price'] == Decimal('1303.1')
            assert record['bid_size'] == Decimal('41.635')
            assert record['ask_size'] == Decimal('28.706')
        elif msg['channel'] == 'trade':
            _, kwargs = exchange.normalised_topics['trades_l3'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'ftx'
            assert record['symbol'] == 'ETH.USD'
            assert record['taker_side'] == 'buy'
            assert record['price'] == Decimal('1303.1')
            assert record['size'] == Decimal('0.109')
            assert record['trade_id'] == '5176374824'
            assert record['event_timestamp'] == 1666172165188
        elif msg['channel'] == 'orderbook':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'ftx'
            assert record['symbol'] == 'ETH.USD'
            assert record['side'] == 'sell'
            assert record['price'] == Decimal('1303.1')
            assert record['size'] == Decimal('28.706')
