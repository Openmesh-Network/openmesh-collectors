import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import BitfinexStandardiser


@pytest.mark.asyncio()
async def test_bitfinex_agent(mock_kafka):
    exchange = BitfinexStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/bitfinex.json'))
    for msg in data:
        await exchange.handle_message(msg)
        if msg[-2] == 'ticker':
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bitfinex'
            assert record['symbol'] == 'ETH.USDT'
            assert record['bid_price'] == Decimal('1312.2')
            assert record['ask_price'] == Decimal('1312.3')
            assert record['bid_size'] == Decimal('356.78077748')
            assert record['ask_size'] == Decimal('345.44388843')
        elif msg[-2] == 'trades':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bitfinex'
            assert record['symbol'] == 'BTC.USDT'
            assert record['taker_side'] == 'buy'
            assert record['price'] == Decimal('19297')
            assert record['size'] == Decimal('0.0805')
            assert record['trade_id'] == '1228532424'
        elif msg[-2] == 'lob_l3':
            _, kwargs = exchange.normalised_topics['lob_l3'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bitfinex'
            assert record['symbol'] == 'ETH.USDT'
            assert record['side'] == 'buy'
            assert record['price'] == Decimal('0')
            assert record['size'] == Decimal('1')
        elif msg[-2] == 'candle':
            _, kwargs = exchange.normalised_topics['candle'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bitfinex'
            assert record['symbol'] == 'BTC.USDT'
            assert record['o'] == Decimal('19296')
            assert record['h'] == Decimal('19297')
            assert record['l'] == Decimal('19296')
            assert record['closed'] is None
            assert record['trades'] == -1
        elif msg[-2] == 'funding_rate':
            _, kwargs = exchange.normalised_topics['funding_rate'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'bitfinex'
            assert record['symbol'] == 'BTC.USDT-PERP'
            assert record['funding_rate'] == Decimal('37206077.41847873')
            assert record['mark_price'] == -1
