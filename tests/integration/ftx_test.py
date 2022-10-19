import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from l3_atom.off_chain import FTX


@pytest.mark.asyncio()
async def test_ftx_connector(teardown_async):
    types = ['orderbook', 'trades', 'ticker']
    ret = []
    FTX.process_message = mock_process_message(ret)
    FTX._init_kafka = Mock()
    connector = FTX()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get('channel', None) in types
    await connector.stop()
