from helpers.read_config import get_redis_config
import sys
import aioredis
from aioredis.client import Pipeline
import asyncio
import json

class RedisProducer:
    def __init__(self, topic):
        self.topic = topic
        conf = get_redis_config()
        self.redis_host = conf['REDIS_HOST']
        self.redis_port = conf['REDIS_PORT']
        self.stream_max_len = int(conf['stream_max_len'])
        self.pool = self.get_redis_pool()
        
    def get_redis_pool(self):
        try:
            pool = aioredis.from_url(
                (f"redis://{self.redis_host}"), encoding='utf-8', decode_responses=True)
            return pool
        except ConnectionRefusedError as e:
            print('cannot connect to redis on:', self.redis_host, self.redis_port)
            return None

    def get_pipe(self):
        if self.pool is None:
            print('cannot connect to redis on:', self.redis_host, self.redis_port)
            return
        return self.pool.pipeline()

    async def produce(self, key, msg):
        if self.pool is None:
            print('cannot connect to redis on:', self.redis_host, self.redis_port)
            return
        if isinstance(msg, dict) or isinstance(msg, list):
            msg = json.dumps(msg).encode('utf-8')
        await self.pool.xadd(self.topic, fields={key: msg}, maxlen=self.stream_max_len, approximate=True)
        return 1

    async def produce_multi(self, key_field, events):
        async with self.pool.pipeline() as pipe:
            for event in events:
                key = event[key_field]
                event = json.dumps(event).encode('utf-8')
                pipe.xadd(self.topic, fields={key: event}, maxlen=self.stream_max_len, approximate=True)
            await pipe.execute()

    def pipeline_produce(self, feed, pipeline, key, msg):
        if isinstance(msg, dict) or isinstance(msg, list):
            msg = json.dumps(msg).encode('utf-8')
        pipeline.xadd(self.topic + feed, fields={key: msg}, maxlen=self.stream_max_len, approximate=True)

    def pipeline_produce_raw(self, pipeline, key, msg):
        self.pipeline_produce('-raw', pipeline, key, msg)
    
    def pipeline_produce_normalised(self, pipeline, key, msg):
        self.pipeline_produce('-normalised', pipeline, key, msg)
    
    def pipeline_produce_trade(self, pipeline, key, msg):
        self.pipeline_produce('-trades', pipeline, key, msg)

    async def consume(self):
        if self.pool is None:
            print('cannot connect to redis on:', self.redis_host, self.redis_port)
            return
        return await self.pool.xreadgroup('group', 'consumer', streams={self.topic: ">", "coinbase-normalised": ">"}, count=self.stream_max_len, block=0)

async def main():
    producer = RedisProducer(sys.argv[1])
    while True:
        print(await producer.consume())

if __name__ == '__main__':
    asyncio.run(main())