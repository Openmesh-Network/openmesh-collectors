import pytest
import json
import asyncio
from contextlib import suppress
from unittest.mock import Mock

from l3_atom.off_chain import Coinbase, Binance, BinanceFutures


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        ret.append(msg)
    return process_message


@pytest.mark.asyncio()
async def test_coinbase_connector():
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
    pending = asyncio.all_tasks()
    t = []
    for task in pending:
        task.cancel()
        t.append(task)
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*t)


@pytest.mark.asyncio()
async def test_binance_connector():
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
    pending = asyncio.all_tasks()
    t = []
    for task in pending:
        task.cancel()
        t.append(task)
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*t)


@pytest.mark.asyncio()
async def test_binance_futures_connector():
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
    pending = asyncio.all_tasks()
    t = []
    for task in pending:
        task.cancel()
        t.append(task)
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*t)
