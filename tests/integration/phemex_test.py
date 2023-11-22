import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Phemex


@pytest.mark.asyncio()
async def test_gemini_connector(teardown_async):
    types = ['trades', 'book', 'kline', 'result', 'id']
    ret = []
    Phemex.process_message = mock_process_message(ret)
    Phemex._init_kafka = Mock()
    connector = Phemex()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert any(t in msg for t in types)
    await connector.stop()
