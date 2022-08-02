import websockets
import asyncio
import time
import json

from normalise.gemini_normalisation import NormaliseGemini
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = "wss://api.gemini.com/v2/marketdata"

async def main():
    producer = RedisProducer("gemini")
    symbols = get_symbols('gemini')
    await connect(url, handle_gemini, producer, symbols)

async def handle_gemini(ws, producer, symbols):
    for symbol in symbols:
        subscribe_message = {
            'type': 'subscribe',
            'subscriptions': [{
                'name': 'l2',
                'symbols': [symbol]
            }]
        }
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseGemini().normalise)

if __name__ == "__main__":
    asyncio.run(main())