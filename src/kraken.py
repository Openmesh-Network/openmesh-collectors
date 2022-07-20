import websockets
import asyncio
import time
import json

from normalise.kraken_normalisation import NormaliseKraken
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://ws.kraken.com'

async def main():
    raw_producer = RedisProducer("kraken-raw")
    normalised_producer = RedisProducer("kraken-normalised")
    trades_producer = RedisProducer("kraken-trades")
    symbols = get_symbols('kraken')
    await connect(url, handle_kraken, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_kraken(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {
                    "event": "subscribe",
                    "pair": [symbol],
                    "subscription": {
                        "name": "book",
                        "depth": 1000
                }
            }
        await ws.send(json.dumps(subscribe_message))

        del subscribe_message["subscription"]["depth"]
        subscribe_message["subscription"]["name"] = "trade"
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseKraken().normalise)

if __name__ == "__main__":
    asyncio.run(main())