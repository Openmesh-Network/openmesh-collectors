import asyncio
import threading
from .kafka_consumer import AsyncKafkaConsumer


# {topic: Event}
events_lock = asyncio.Lock()
events = {}

async def get_consumer(topic):
    consumer = AsyncKafkaConsumer(topic)
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