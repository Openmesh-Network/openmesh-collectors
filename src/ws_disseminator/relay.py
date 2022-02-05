import websockets
import asyncio
import json
import time
import bisect

from .kafka_consumers.consumer_registry import ConsumerRegistry

# {topic_id: KafkaConsumer}
broadcaster_lock = asyncio.Lock()
topic_broadcasters = {}

# {topic_id: [ClientHandler, ClientHandler ...]}
subscriptions_lock = asyncio.Lock()
client_subscriptions = {}

tasks_lock = asyncio.Lock()
tasks = {}

published_lock = asyncio.Lock()
n_published = 0

async def subscribe(topic_id: str, client):
    await broadcaster_lock.acquire()
    await subscriptions_lock.acquire()

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

    subscriptions_lock.release()
    broadcaster_lock.release()

async def unsubscribe(topic_id: str, client):
    await broadcaster_lock.acquire()
    await subscriptions_lock.acquire()

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

    subscriptions_lock.release()
    broadcaster_lock.release()

async def run_topic(topic_id):
    global n_published
    while True:
        msg = await topic_broadcasters[topic_id].get_msg()
        publish(topic_id, msg)
        async with published_lock:
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
            await consumer.shutdown()
    async with tasks_lock:
        for task in tasks.values():
            task.cancel()

async def debug():
    print(f"---------------ts: {int(time.time())}---------------")
    # I originally locked broadcaster_lock here, but it breaks the program so I removed it.
    # May have created an asynchronous race condition. Will test this.
    x = sum(map(lambda x: x.msg_queue.qsize(), topic_broadcasters.values()))
    print(f"Consumer Backlog: {x}")
    print(f"Messages Broadcasted: {n_published}")
    print(f"Broadcasters: {topic_broadcasters}")
    print(f"Subscribers: {client_subscriptions}")
    print(f"--------------------------------------------\n")