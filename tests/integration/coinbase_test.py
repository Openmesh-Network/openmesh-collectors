import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Coinbase


@pytest.mark.asyncio()
async def test_coinbase_connector(teardown_async):
    types = ['open', 'done', 'ticker', 'match',
             'change', 'subscriptions', 'received']
    ret = []
    Coinbase.process_message = mock_process_message(ret)
    Coinbase._init_kafka = Mock()
    connector = Coinbase()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get('type', None) in types
    await connector.stop()
