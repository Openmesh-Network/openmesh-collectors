import aiohttp
import asyncio

async def get_snapshot(snapshot_url):
    async with aiohttp.ClientSession() as session:
        async with session.get(snapshot_url) as resp:
            return await resp.text()