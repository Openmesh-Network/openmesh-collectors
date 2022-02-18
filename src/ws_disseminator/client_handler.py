import asyncio
import json

from . import relay, parse_message
from .logger import log

client_id = 0

class ClientHandler():
    def __init__(self, ws, client_id=-1):
        self.id = client_id
        self.ws = ws 
        self.subscriptions = set()
        self.lock = asyncio.Lock()
        self.closed = False
    
    def __repr__(self):
        return f"<ClientHandler: id={self.id}>"
    
    def get_id(self):
        return self.id
    
    def get_subs(self):
        return self.subscriptions
    
    def add_sub(self, sub):
        self.subscriptions.add(sub)

    def remove_sub(self, sub):
        self.subscriptions.remove(sub)
    
    def get_ws(self):
        return self.ws
    
    async def shutdown(self):
        self.closed = True
        for sub in self.subscriptions:
            await relay.unsubscribe(sub, self)
        await self.ws.close()
    
    async def lock(self):
        await self.lock.acquire()
    
    def unlock(self):
        self.lock.release()
    
    async def send_json(self, msg):
        await self.ws.send(json.dumps(msg))

async def handle_ws(ws):
    global client_id
    client = ClientHandler(ws, client_id = client_id)
    client_id += 1
    try:
        listener = asyncio.create_task(_listen(client))
        poller = asyncio.create_task(_poll(client))
        done, pending = await asyncio.wait(
            [listener, poller],
            return_when = asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
    except Exception as e:
        print(e)
    finally:
        if not client.closed:
            await client.shutdown()
            # print(f"client_{client.id} shutdown")

async def _poll(client):
    await client.get_ws().wait_closed()
    
async def _listen(client):
    ws = client.get_ws()
    # print(f"client_{client.get_id()} connected")
    async for message in ws:
        parse_code = parse_message.is_valid(message)
        if parse_code != 0:
            log("client_handler", f"client_{client.id}: {message}, error_code: {parse_code}")
            await ws.send(json.dumps({"event": "error", "code": parse_code}))
        elif parse_message.is_subscribe(message):
            topic = json.loads(message)['topic']
            if topic in client.get_subs():
                # log("client_handler", f"client_{client.id} already subscribed to {topic}")
                continue
            await ws.send(json.dumps({"event": "subscribed", "topic": topic}))
            await relay.subscribe(topic, client)
            client.add_sub(topic)
        elif parse_message.is_unsubscribe(message):
            topic = json.loads(message)['topic']
            if topic not in client.get_subs():
                # log("client_handler", f"client_{client_id} not subscribed to {topic}")
                continue
            await ws.send(json.dumps({"event": "unsubscribed", "topic": topic}))
            await relay.unsubscribe(topic, client)
            client.remove_sub(topic)
        else:
            log("client_handler", f"client_{client.id}: {message}")
            await ws.send(json.dumps({"event": "error", "code": 7}))