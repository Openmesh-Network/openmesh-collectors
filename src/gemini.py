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
    raw_producer = RedisProducer("gemini-raw")
    normalised_producer = RedisProducer("gemini-normalised")
    trades_producer = RedisProducer("gemini-trades")
    symbols = get_symbols('gemini')
    await connect(url, handle_gemini, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_gemini(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {
            'type': 'subscribe',
            'subscriptions': [{
                'name': 'l2',
                'symbols': [symbol]
            }]
        }
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseGemini().normalise)

if __name__ == "__main__":
    asyncio.run(main())