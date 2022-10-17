from unittest.mock import Mock
import pytest
from l3_atom.stream_processing import app, codecs
import pytest_asyncio
import asyncio
from contextlib import suppress


def new_config():
    return {
        'KAFKA_BOOTSTRAP_SERVERS': None,
        'SCHEMA_REGISTRY_URL': None
    }


@pytest.fixture()
def mock_kafka():
    app.get_kafka_config = new_config
    codecs.initialise = Mock()


@pytest_asyncio.fixture()
async def teardown_async():
    yield
    pending = asyncio.all_tasks()
    t = []
    for task in pending:
        task.cancel()
        t.append(task)
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*t)


@pytest.fixture()
def test_app(event_loop):
    f_app = app.init()
    f_app.finalize()
    f_app.conf.store = 'memory://'
    f_app.flow_control.resume()
    return f_app
