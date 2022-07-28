import websockets
import asyncio
import time
import json

from normalise.kraken_futures_normalisation import NormaliseKrakenFutures
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://futures.kraken.com/ws/v1'

async def main():
    producer = RedisProducer("kraken-futures")
    symbols = get_symbols('kraken-futures')
    await connect(url, handle_kraken_futures, producer, symbols)

async def handle_kraken_futures(ws, producer, symbols):
    subscribe_message = {
        "event": "subscribe",
        "feed": "book",
        "product_ids": symbols
    }
    await ws.send(json.dumps(subscribe_message))
    subscribe_message["feed"] = "trade"
    await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseKrakenFutures().normalise)

if __name__ == "__main__":
    asyncio.run(main())