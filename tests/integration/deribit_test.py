import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Deribit


@pytest.mark.asyncio()
async def test_deribit_connector(teardown_async):
    types = ['trades', 'book', 'ticker', 'chart']
    ret = []
    Deribit.process_message = mock_process_message(ret)
    Deribit._init_kafka = Mock()
    connector = Deribit()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert 'result' in msg or msg['params']['channel'].split('.')[0] in types
    await connector.stop()
