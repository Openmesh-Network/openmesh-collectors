import asyncio
import threading
from .kafka_consumer import KafkaConsumer


class AsyncConsumer:
    def __init__(self, kafka_consumer):
        """Wrap a synchronous kafka consumer in this class to make it asynchronous"""
        self.waiter = asyncio.Future()
        self.kafka_consumer = kafka_consumer
        self.consumer_lock = asyncio.Lock()
        self.q_size = 0
    
    def __repr__(self):
        return f"AsyncConsumer: topic={self.kafka_consumer.topic}"

    def publish(self, value):
        waiter, self.waiter = self.waiter, asyncio.Future()
        waiter.set_result((value, self.waiter))
    
    async def run_consumer(self, stop):
        while not stop.is_set():
            async with self.consumer_lock:
                data = await asyncio.to_thread(self.kafka_consumer.consume)
            if data:
                self.publish(data)
                self.q_size += 1
    
    async def size(self):
        return self.q_size
    
    async def shutdown(self):
        async with self.consumer_lock:
            await asyncio.to_thread(self.kafka_consumer.shutdown)

    async def get_msg(self):
        waiter = self.waiter
        while True:
            value, waiter = await waiter
            self.q_size -= 1
            yield value

    __aiter__ = get_msg


# {topic: Event}
events_lock = asyncio.Lock()
events = {}

async def get_consumer(topic):
    consumer = AsyncConsumer(KafkaConsumer(topic))
    event = threading.Event()
    asyncio.create_task(consumer.run_consumer(event))
    events[topic] = {'consumer': consumer, 'event': event}
    print(f"{topic} consumer started")
    return consumer

async def shutdown_topic(topic):
    async with events_lock:
        events[topic]['event'].set()
        await events[topic]['consumer'].shutdown()
        del events[topic]
    print(f"{topic} consumer stopped")

async def shutdown():
    async with events_lock:
        for topic in events.keys():
            events[topic]['event'].set()
            await events[topic]['consumer'].shutdown()