import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import BybitStandardiser


@pytest.mark.asyncio()
async def test_bybit_agent(mock_kafka):
    exchange = BybitStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/bybit.json'))
    for msg in data:
        topic = msg['topic']
        await exchange.handle_message(msg)
        if topic.startswith('bookticker'):
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bybit'
            assert record['symbol'] == 'BTC.USDT'
            assert record['bid_price'] == Decimal('19167')
            assert record['ask_price'] == Decimal('19169')
            assert record['bid_size'] == Decimal('0.0001')
            assert record['ask_size'] == Decimal('0.000411')
        elif topic.startswith('trade'):
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bybit'
            assert record['symbol'] == 'BTC.USDT'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19168')
            assert record['size'] == Decimal('0.0001')
            assert record['trade_id'] == '2290000000019026549'
        elif topic.startswith('orderbook'):
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bybit'
            assert record['symbol'] == 'BTC.USDT'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('19167')
            assert record['size'] == Decimal('0.0001')
        elif topic.startswith('kline'):
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bybit'
            assert record['symbol'] == 'BTC.USDT'
            assert record['o'] == Decimal('19173')
            assert record['h'] == Decimal('19173')
            assert record['l'] == Decimal('19168')
            assert record['closed'] is False
            assert record['trades'] == -1
