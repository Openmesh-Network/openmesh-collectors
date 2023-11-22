import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import BinanceFutures


@pytest.mark.asyncio()
async def test_binance_futures_connector(teardown_async):
    types = ['bookTicker', 'aggTrade', 'depthUpdate', 'kline', 'markPriceUpdate']
    ret = []
    BinanceFutures.process_message = mock_process_message(ret)
    BinanceFutures._init_kafka = Mock()
    connector = BinanceFutures()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get(
            'e', None) in types or 'openInterest' in msg or 'result' in msg
    await connector.stop()
