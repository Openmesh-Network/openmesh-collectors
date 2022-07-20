import websockets
import asyncio
import time
import json

from normalise.phemex_normalisation import NormalisePhemex
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://phemex.com/ws'

async def main():
    raw_producer = RedisProducer("phemex-raw")
    normalised_producer = RedisProducer("phemex-normalised")
    trades_producer = RedisProducer("phemex-trades")
    symbols = get_symbols('phemex')
    await connect(url, handle_phemex, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_phemex(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {
                "id": 1234,  # random id
                "method": "orderbook.subscribe",
                "params": [symbol]
            }
        await ws.send(json.dumps(subscribe_message))

        subscribe_message['method'] = "trade.subscribe"
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormalisePhemex().normalise)

if __name__ == "__main__":
    asyncio.run(main())