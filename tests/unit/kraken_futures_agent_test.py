import pytest
from unittest.mock import AsyncMock, Mock
import json
from decimal import Decimal
from openmesh.stream_processing.standardisers import KrakenFuturesStandardiser


@pytest.mark.asyncio()
async def test_kraken_futures_agent(mock_kafka):
    exchange = KrakenFuturesStandardiser()
    exchange.start_exchange()
    for topic in exchange.normalised_topics:
        exchange.normalised_topics[topic] = Mock()
        exchange.normalised_topics[topic].send = AsyncMock()
    data = json.load(open('mock_data/kraken_futures.json'))
    for msg in data:
        await exchange.handle_message(msg)
        t = msg['feed']
        if t == 'book':
            _, kwargs = exchange.normalised_topics['lob'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken-futures'
            assert record['symbol'] == 'BTC.USD-PERP'
            assert record['side'] == 'sell'
            assert record['price'] == Decimal('19141')
        if t == 'trade':
            _, kwargs = exchange.normalised_topics['trades'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken-futures'
            assert record['symbol'] == 'BTC.USD-PERP'
            assert record['taker_side'] == 'sell'
            assert record['price'] == Decimal('19113')
            assert record['size'] == Decimal('0.0005')
            assert record['trade_id'] == 'd869af5f-1926-4ec4-9ec4-95e184daa6f8'
        if t == 'ticker':
            _, kwargs = exchange.normalised_topics['ticker'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken-futures'
            assert record['symbol'] == 'ETH.USD-PERP'
            assert record['bid_price'] == Decimal('1289.2')
            assert record['ask_price'] == Decimal('1289.6')
            assert record['bid_size'] == Decimal('9.326')

            _, kwargs = exchange.normalised_topics['funding_rate'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken-futures'
            assert record['symbol'] == 'ETH.USD-PERP'
            assert record['funding_rate'] == Decimal('-0.01637028178319473')
            assert record['mark_price'] == Decimal('1289.4')

            _, kwargs = exchange.normalised_topics['open_interest'].send.call_args
            record = kwargs['value'].asdict()
            assert record['exchange'] == 'kraken-futures'
            assert record['symbol'] == 'ETH.USD-PERP'
            assert record['open_interest'] == Decimal('2868.275')
