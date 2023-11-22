import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Binance


@pytest.mark.asyncio()
async def test_binance_connector(teardown_async):
    types = ['bookTicker', 'trade', 'depthUpdate', 'kline']
    ret = []
    Binance.process_message = mock_process_message(ret)
    Binance._init_kafka = Mock()
    connector = Binance()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get('e', None) in types or 'result' in msg or 'A' in msg
    await connector.stop()
