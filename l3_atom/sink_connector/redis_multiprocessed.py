import aioredis
from yapic import json
from l3_atom.sink_connector.sink_connector import SinkConnector, SinkMessageHandler
from l3_atom.helpers.read_config import get_redis_config
import logging

class RedisConnector(SinkMessageHandler):
    def __init__(self, exchange:str):
        super().__init__(exchange)
        conf = get_redis_config()
        self.redis_host = conf['REDIS_HOST']
        self.redis_port = conf['REDIS_PORT']
        self.stream_max_len = int(conf['stream_max_len'])
        self.url = f"redis://{self.redis_host}:{self.redis_port}"

class RedisStreamsConnector(RedisConnector, SinkConnector):
    async def producer(self):
        conn = aioredis.from_url(self.url)

        while self.started:
            async with self.read_from_pipe() as messages:
                async with conn.pipeline(transaction=False) as pipe:
                    for message in messages:
                        pipe.xadd(f"{self.exchange}-raw", fields={'0':message}, maxlen=self.stream_max_len, approximate=True)
                    await pipe.execute()
        await conn.close()
        await conn.connection_pool.disconnect()