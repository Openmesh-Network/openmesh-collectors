from unittest.mock import Mock
import pytest
from openmesh.stream_processing import app, codecs
import pytest_asyncio
import asyncio
from contextlib import suppress
import logging


def new_config():
    return {
        'KAFKA_BOOTSTRAP_SERVERS': None,
        'SCHEMA_REGISTRY_URL': None
    }


@pytest.fixture()
def mock_kafka():
    app.get_kafka_config = new_config
    codecs.initialise = Mock()


@pytest.fixture()
async def setup_async():
    logging.getLogger('asyncio').setLevel(logging.CRITICAL)


@pytest_asyncio.fixture()
async def teardown_async():
    yield
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    t = []
    for task in pending:
        task.cancel()
        t.append(task)
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*t)


@pytest.fixture()
def test_app(event_loop):
    f_app = app.init()
    return f_app
