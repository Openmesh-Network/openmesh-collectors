import pytest
from util import mock_process_message
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import KrakenFutures


@pytest.mark.asyncio()
async def test_kraken_futures_connector(teardown_async):
    types = ['book', 'trade', 'ticker', 'book_snapshot', 'trade_snapshot']
    ret = []
    KrakenFutures.process_message = mock_process_message(ret)
    KrakenFutures._init_kafka = Mock()
    connector = KrakenFutures()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg.get('feed', None) in types or msg.get('event', None) in ('info', 'subscribed')
    await connector.stop()
