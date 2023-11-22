import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import BinanceFuturesStandardiser


@pytest.mark.asyncio()
async def test_binance_futures_agent(mock_kafka):
    exchange = BinanceFuturesStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/binance_futures.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if 'openInterest' in msg:
            _, kwargs = exchange.normalised_topics['open_interest'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance-futures'
            assert record['symbol'] == 'BTC.USDT-PERP'
            assert record['open_interest'] == Decimal('145493.197')
        elif msg['e'] == 'markPriceUpdate':
            _, kwargs = exchange.normalised_topics['funding_rate'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance-futures'
            assert record['symbol'] == 'ETH.USDT-PERP'
            assert record['mark_price'] == Decimal('1323.72000000')
            assert record['funding_rate'] == Decimal('-0.00000304')
            assert record['next_funding_time'] == 1665388800000
        elif msg['e'] == 'aggTrade':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'binance-futures'
            assert record['symbol'] == 'BTC.USDT-PERP'
            assert record['taker_side'] == 'buy'
            assert record['price'] == Decimal('0.001')
            assert record['size'] == Decimal('100')
            assert record['trade_id'] == '5933014'
