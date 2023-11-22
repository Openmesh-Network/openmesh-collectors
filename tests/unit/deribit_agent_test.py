import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import DeribitStandardiser


@pytest.mark.asyncio()
async def test_deribit_agent(mock_kafka):
    exchange = DeribitStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/deribit.json'))
    for msg in data:
        await exchange.handle_message(msg)
        t = msg['params']['channel'].split('.')[0]
        if t == 'book':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'BTC.USDC-PERP'
            assert record['side'] == 'sell'
            assert record['price'] == Decimal('19341')
        if t == 'trades':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'BTC.USDC-PERP'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19293')
            assert record['size'] == Decimal('0.002')
            assert record['trade_id'] == 'USDC-5560136'
        if t == 'chart':
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'ETH.USDC-PERP'
            assert record['o'] == Decimal('1306.3')
            assert record['h'] == Decimal('1306.3')
            assert record['v'] == Decimal('0')
        if t == 'ticker':
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'BTC.USDC-PERP'
            assert record['bid_price'] == Decimal('19299')
            assert record['ask_price'] == Decimal('19301')
            assert record['bid_size'] == Decimal('0.157')

            _, kwargs = exchange.normalised_topics['funding_rate'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'BTC.USDC-PERP'
            assert record['funding_rate'] == Decimal('0')
            assert record['mark_price'] == Decimal('19298.9677')

            _, kwargs = exchange.normalised_topics['open_interest'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'deribit'
            assert record['symbol'] == 'BTC.USDC-PERP'
            assert record['open_interest'] == Decimal('348.718')
