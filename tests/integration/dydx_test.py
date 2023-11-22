import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Dydx


@pytest.mark.asyncio()
async def test_dydx_connector(teardown_async):
    types = ['v3_trades', 'v3_orderbook']
    ret = []
    Dydx.process_message = mock_process_message(ret)
    Dydx._init_kafka = Mock()
    connector = Dydx()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get(
            'channel', None) in types or msg.get('type', None) == 'connected'
    await connector.stop()
