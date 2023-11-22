import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import KrakenStandardiser


@pytest.mark.asyncio()
async def test_kraken_agent(mock_kafka):
    exchange = KrakenStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/kraken.json'))
    for msg in data:
        await exchange.handle_message(msg)
        t = msg[-3]
        if t.startswith('book'):
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken'
            assert record['symbol'] == 'BTC.USD'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('19032.40000')
        if t == 'trade':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken'
            assert record['symbol'] == 'BTC.USD'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19047.10000')
            assert record['size'] == Decimal('0.00360000')
        if t.startswith('ohlc'):
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken'
            assert record['symbol'] == 'BTC.USD'
            assert record['o'] == Decimal('19048.30000')
            assert record['h'] == Decimal('19048.90000')
            assert record['v'] == Decimal('0.36745406')
        if t == 'ticker':
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken'
            assert record['symbol'] == 'ETH.USD'
            assert record['bid_price'] == Decimal('1280.55000')
            assert record['ask_price'] == Decimal('1280.56000')
            assert record['bid_size'] == Decimal('96.12473715')
