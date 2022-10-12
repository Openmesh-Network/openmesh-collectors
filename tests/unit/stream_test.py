import pytest
from unittest.mock import AsyncMock, Mock
import json

from l3_atom.stream_processing import app, codecs
from decimal import Decimal


def new_config():
    return {
        'KAFKA_BOOTSTRAP_SERVERS': None,
        'SCHEMA_REGISTRY_URL': None
    }


@pytest.fixture()
def mock_kafka():
    app.get_kafka_config = new_config
    codecs.initialise = Mock()


@pytest.fixture()
def test_app(event_loop):
    f_app = app.init()
    f_app.finalize()
    f_app.conf.store = 'memory://'
    f_app.flow_control.resume()
    return f_app


@pytest.mark.asyncio()
async def test_coinbase_agent(mock_kafka, test_app):
    async with test_app.agents['coinbase_agent'].test_context() as agent:
        topics = agent.fun.__self__.normalised_topics
        for topic in topics.values():
            topic.send = AsyncMock()
        data = json.load(open('mock_data/coinbase.json'))
        for msg in data:
            await agent.put(msg)
            if msg['type'] == 'open':
                _, kwargs = topics['lob_l3'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'coinbase'
                assert record['symbol'] == 'BTC.USD'
                assert record['side'] == 'buy'
                assert record['price'] == Decimal('19466.79')
            if msg['type'] == 'done':
                _, kwargs = topics['lob_l3'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'coinbase'
                assert record['symbol'] == 'BTC.USD'
                assert record['side'] == 'buy'
                assert record['price'] == Decimal('19466.52')
            if msg['type'] == 'ticker':
                _, kwargs = topics['ticker'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'coinbase'
                assert record['symbol'] == 'BTC.USD'
                assert record['bid_price'] == Decimal('19467.66')
                assert record['ask_price'] == Decimal('19468.92')
            if msg['type'] == 'match':
                _, kwargs = topics['trades_l3'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'coinbase'
                assert record['symbol'] == 'BTC.USD'
                assert record['taker_side'] == 'buy'
                assert record['price'] == Decimal('19413.12')
                assert record['size'] == Decimal('0.00149926')
                assert record['maker_order_id'] == '548d0bfa-ae4e-4a6b-9448-4203a686805f'
                assert record['trade_id'] == '428981156'
            if msg['type'] == 'change':
                _, kwargs = topics['lob_l3'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'coinbase'
                assert record['symbol'] == 'BTC.USD'
                assert record['side'] == 'sell'
                assert record['price'] == Decimal('400.23')
                assert record['size'] == Decimal('5.23512')
                assert record['order_id'] == 'ac928c66-ca53-498f-9c13-a110027a60e8'


@pytest.mark.asyncio()
async def test_binance_agent(mock_kafka, test_app):
    async with test_app.agents['binance_agent'].test_context() as agent:
        topics = agent.fun.__self__.normalised_topics
        for topic in topics.values():
            topic.send = AsyncMock()
        data = json.load(open('mock_data/binance.json'))
        for msg in data:
            await agent.put(msg)
            if 'A' in msg:
                _, kwargs = topics['ticker'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance'
                assert record['symbol'] == 'BTC.USDT'
                assert record['bid_price'] == Decimal('0.0432345')
                assert record['ask_price'] == Decimal('0.765000')
                assert record['bid_size'] == Decimal('50.00000000')
                assert record['ask_size'] == Decimal('32.40000000')
            elif msg['e'] == 'trade':
                _, kwargs = topics['trades_l3'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance'
                assert record['symbol'] == 'ETH.BTC'
                assert record['taker_side'] == 'sell'
                assert record['price'] == Decimal('0.00861')
                assert record['size'] == Decimal('4')
                assert record['taker_order_id'] == '123456785'
                assert record['trade_id'] == '3543'
            elif msg['e'] == 'depthUpdate':
                _, kwargs = topics['lob'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance'
                assert record['symbol'] == 'ETH.USDT'
                assert record['side'] == 'buy'
                assert record['price'] == Decimal('1320.06000000')
                assert record['size'] == Decimal('11.62390000')
            elif msg['e'] == 'kline':
                _, kwargs = topics['candle'].send.call_args
                record = kwargs['value'].asdict()
                assert record['exchange'] == 'binance'
                assert record['symbol'] == 'BTC.USDT'
                assert record['o'] == Decimal('19453.47000000')
                assert record['h'] == Decimal('19453.47000000')
                assert record['l'] == Decimal('19453.03000000')
                assert record['closed'] is True
                assert record['trades'] == 7


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
