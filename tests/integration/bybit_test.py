import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Bybit


@pytest.mark.asyncio()
async def test_bybit_connector(teardown_async):
    types = ['bookticker', 'trade', 'orderbook', 'kline']
    ret = []
    Bybit.process_message = mock_process_message(ret)
    Bybit._init_kafka = Mock()
    connector = Bybit()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert 'success' in msg or msg['topic'].split('.')[0] in types
    await connector.stop()
