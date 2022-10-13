import pytest_asyncio
import asyncio
from contextlib import suppress

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