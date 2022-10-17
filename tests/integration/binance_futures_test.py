import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from l3_atom.off_chain import BinanceFutures


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
