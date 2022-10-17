import pytest
from unittest.mock import AsyncMock
import json
from decimal import Decimal


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
