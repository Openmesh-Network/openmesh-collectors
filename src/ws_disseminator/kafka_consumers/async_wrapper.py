import asyncio


class AsyncConsumer:
    def __init__(self, kafka_consumer):
        """Wrap a synchronous kafka consumer in this class to make it asynchronous"""
        self.waiter = asyncio.Future()
        self.kafka_consumer = kafka_consumer


    def publish(self, value):
        waiter, self.waiter = self.waiter, asyncio.Future()
        waiter.set_result((value, self.waiter))
    
    def run_consumer(self):
        while True:
            data = self.kafka_consumer.consume()
            if data:
                self.publish(data)
    
    async def start_ws(self):
        self.consumer_task = asyncio.to_thread(self.run_consumer)
        await self.consumer_task
        
    async def shutdown(self):
        self.consumer_task.cancel()

    async def get(self):
        waiter = self.waiter
        while True:
            value, waiter = await waiter
            yield value

    __aiter__ = get