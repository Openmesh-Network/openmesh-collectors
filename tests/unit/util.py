import pytest_asyncio
import asyncio
from contextlib import suppress

@pytest_asyncio.fixture()
async def teardown_async():
    yield
    pending = asyncio.all_tasks()
    for task in pending:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task