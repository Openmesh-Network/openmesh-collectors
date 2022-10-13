import pytest
from util import teardown_async
import json
import asyncio
from unittest.mock import Mock

from l3_atom.off_chain import Coinbase, Binance, BinanceFutures, Dydx


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        ret.append(msg)
    return process_message

@pytest.mark.asyncio()
async def test_coinbase_connector(teardown_async):
    types = ['open', 'done', 'ticker', 'match',
             'change', 'subscriptions', 'received']
    ret = []
    Coinbase.process_message = mock_process_message(ret)
    Coinbase._init_kafka = Mock()
    connector = Coinbase()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    for msg in ret:
        assert msg.get('type', None) in types
    await connector.stop()


@pytest.mark.asyncio()
async def test_binance_connector(teardown_async):
    types = ['bookTicker', 'trade', 'depthUpdate', 'kline']
    ret = []
    Binance.process_message = mock_process_message(ret)
    Binance._init_kafka = Mock()
    connector = Binance()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    for msg in ret:
        assert msg.get('e', None) in types or 'result' in msg or 'A' in msg
    await connector.stop()


@pytest.mark.asyncio()
async def test_binance_futures_connector(teardown_async):
    types = ['bookTicker', 'trade', 'depthUpdate', 'kline', 'markPriceUpdate']
    ret = []
    BinanceFutures.process_message = mock_process_message(ret)
    BinanceFutures._init_kafka = Mock()
    connector = BinanceFutures()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    for msg in ret:
        assert msg.get(
            'e', None) in types or 'openInterest' in msg or 'result' in msg
    await connector.stop()

@pytest.mark.asyncio()
async def test_Dydx_connector():
    types = ['v3_trades', 'v3_orderbook']
    ret = []
    Dydx.process_message = mock_process_message(ret)
    Dydx._init_kafka = Mock()
    connector = Dydx()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    await asyncio.sleep(1)
    for msg in ret:
        assert msg.get(
            'channel', None) in types or msg.get('type', None) == 'connected'
    await connector.stop()
