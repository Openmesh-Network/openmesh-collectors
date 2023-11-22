import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import ApolloX


@pytest.mark.asyncio()
async def test_apollox_connector(teardown_async):
    types = ['bookTicker', 'trade', 'depthUpdate', 'kline', 'markPriceUpdate']
    ret = []
    ApolloX.process_message = mock_process_message(ret)
    ApolloX._init_kafka = Mock()
    connector = ApolloX()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get(
            'e', None) in types or 'openInterest' in msg or 'result' in msg
    await connector.stop()
