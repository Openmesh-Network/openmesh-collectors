import pytest
from unittest.mock import AsyncMock
import json
import asyncio

from l3_atom.off_chain import Coinbase, Binance, BinanceFutures

def mock_process_message(types:list, type_field:str, **kwargs):
    async def process_message(cls, msg):
        msg = json.loads(msg)
        assert msg.get(type_field, None) in types or 'open_interest' in msg
    return process_message

@pytest.mark.asyncio()
async def test_coinbase_connector():
    connector = Coinbase()
    types = ['open', 'done', 'ticker', 'match', 'change']
    connector.process_message = mock_process_message(types=types, type_field='type')
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    await connector.stop()

@pytest.mark.asyncio()
async def test_binance_connector():
    connector = Binance()
    types = ['depthUpdate', 'trade', 'ticker']
    connector.process_message = mock_process_message(types=types, type_field='e')
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    await connector.stop()

@pytest.mark.asyncio()
async def test_binance_futures_connector():
    connector = BinanceFutures()
    types = ['depthUpdate', 'trade', 'ticker', 'markPriceUpdate']
    connector.process_message = mock_process_message(types=types, type_field='e')
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    await connector.stop()