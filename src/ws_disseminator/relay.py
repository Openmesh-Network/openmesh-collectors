from threading import Lock
import websockets
import asyncio
import json
import time

from .kafka_consumers.consumer_registry import ConsumerRegistry

# {topic_id: KafkaConsumer}
broadcaster_lock = asyncio.Lock()
topic_broadcasters = {}

# {topic_id: [ClientHandler, ClientHandler ...]}
subscriptions_lock = asyncio.Lock()
client_subscriptions = {}

tasks_lock = asyncio.Lock()
tasks = {}

published_lock = Lock()
n_published = 0

async def subscribe(topic_id: str, client):
    async with broadcaster_lock:
        async with subscriptions_lock:
            if topic_id not in topic_broadcasters.keys():
                assert topic_id not in client_subscriptions.keys()
                topic_broadcasters[topic_id] = await ConsumerRegistry().get(topic_id)
                client_subscriptions[topic_id] = [client]

                task = asyncio.create_task(run_topic(topic_id))
                print(f"{topic_id} consumer started")
                async with tasks_lock:
                    tasks[topic_id] = task
            else: 
                client_subscriptions[topic_id].append(client)
            print(f"Subscribed to {topic_id}")

async def unsubscribe(topic_id: str, client):
    async with broadcaster_lock:
        async with subscriptions_lock:
            _check_topic_present(topic_id)
            if client not in client_subscriptions[topic_id]:
                print(f"client is not subscribed to {topic_id}")
                return

            client_subscriptions[topic_id].remove(client)
            if len(client_subscriptions[topic_id]) == 0:
                async with tasks_lock:
                    await topic_broadcasters[topic_id].shutdown()
                    tasks[topic_id].cancel()
                    del tasks[topic_id]

                del client_subscriptions[topic_id]
                del topic_broadcasters[topic_id]
                print(f"{topic_id} consumer stopped")
            print(f"Unsubscribed from {topic_id}")

async def run_topic(topic_id):
    await topic_broadcasters[topic_id].subscribe()
    await asyncio.gather(
        publish_task(topic_id), 
        topic_broadcasters[topic_id].recv()
    )

async def publish_task(topic_id):
    global n_published
    while True:
        msg = await topic_broadcasters[topic_id].get_msg()
        publish(topic_id, msg)
        with published_lock:
            n_published += 1

def publish(topic_id: str, msg: dict):
    _check_topic_present(topic_id)
    subs = client_subscriptions[topic_id]
    sockets = map(lambda client: client.get_ws(), subs)
    msg = json.dumps(msg).encode('utf-8')
    websockets.broadcast(sockets, msg)

def _check_topic_present(topic_id):
    assert topic_id in topic_broadcasters.keys()
    assert topic_id in client_subscriptions.keys()

async def shutdown():
    async with broadcaster_lock:
        for consumer in topic_broadcasters.values():
            consumer.shutdown()
    async with tasks_lock:
        for task in tasks.values():
            task.cancel()

def debug():
    print(f"---------------ts: {int(time.time())}---------------")
    print(f"Messages Broadcasted: {n_published}")
    print(f"Broadcasters: {topic_broadcasters}")
    print(f"Subscribers: {client_subscriptions}")
    print(f"--------------------------------------------\n")