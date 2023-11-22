import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Gemini


@pytest.mark.asyncio()
async def test_gemini_connector(teardown_async):
    types = ['l2_updates', 'trade', 'candles_1m_updates', 'heartbeat']
    ret = []
    Gemini.process_message = mock_process_message(ret)
    Gemini._init_kafka = Mock()
    connector = Gemini(symbols=['BTC.USD', 'ETH.USD'])
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get('type', None) in types
    await connector.stop()
