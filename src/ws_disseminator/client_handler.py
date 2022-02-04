import asyncio

from . import relay

client_id = 0

class ClientHandler():
    def __init__(self, ws, client_id=-1):
        self.id = client_id
        self.ws = ws 
        self.subscriptions = set()
    
    def __repr__(self):
        return f"<ClientHandler: id={self.id}>"
    
    def get_subs(self):
        return self.subscriptions
    
    def add_sub(self, sub):
        self.subscriptions.add(sub)

    def remove_sub(self, sub):
        self.subscriptions.remove(sub)
    
    def get_ws(self):
        return self.ws
    
    async def shutdown(self):
        for sub in self.subscriptions:
            await relay.unsubscribe(sub, self)

    def is_subscribe(self, msg: str):
        return "sub" in msg

    def is_unsubscribe(self, msg: str):
        return "dc" in msg

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
        await client.shutdown()
        print(f"client_{client.id} Shutdown")

async def _poll(client):
    await client.get_ws().wait_closed()
    
async def _listen(client):
    ws = client.get_ws()
    print("Client Connected")
    async for message in ws:
        if client.is_subscribe(message):
            # Currently hard coded while I identify race conditions within the relay
            message = {'topic': 'bybit'}
            if message['topic'] in client.get_subs():
                print(f"client_{client.id} already subscribed to {message['topic']}")
                continue
            await relay.subscribe(message['topic'], client)
            client.add_sub(message['topic'])
        elif client.is_unsubscribe(message):
            message = {'topic': 'bybit'}
            if message['topic'] not in client.get_subs():
                print(f"client_{client_id} not subscribed to {message['topic']}")
                continue
            await relay.unsubscribe(message['topic'], client)
            client.remove_sub(message['topic'])
        else:
            print(f"client_{client.id}: {message}")