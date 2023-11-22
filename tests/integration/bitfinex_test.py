import pytest
import json
import asyncio
from unittest.mock import Mock

from openmesh.off_chain import Bitfinex

# Bitfinex's process message adds some metadata, so we have to replicate it


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        chan_id = msg[0]
        if isinstance(chan_id, int):
            channel, symbol = cls.chan_ids[chan_id]
            msg.append(channel)
            msg.append(symbol)
        else:
            msg = msg[0]
            symbol = msg.pop(0)
            msg.append('open_interest')
            msg.append(symbol)
        ret.append(msg)
    return process_message


@pytest.mark.asyncio()
async def test_bitfinex_connector(teardown_async):
    types = [*Bitfinex.ws_channels.keys(), *Bitfinex.rest_channels.keys()]
    ret = []
    Bitfinex.process_message = mock_process_message(ret)
    Bitfinex._init_kafka = Mock()
    connector = Bitfinex()
    loop = asyncio.get_event_loop()
    connector.start(loop)
    # Bitfinex has a slow startup, so we'll wait a bit longer than usual
    while len(ret) < 10:
        await asyncio.sleep(0.1)
    for msg in ret:
        assert msg[-2] in types
    await connector.stop()
