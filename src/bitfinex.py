import websockets
import asyncio
import time
import json

from normalise.bitfinex_normalisation import NormaliseBitfinex
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = "wss://api-pub.bitfinex.com/ws/2"

async def main():
    producer = RedisProducer("bitfinex")
    symbols = get_symbols('bitfinex')
    await connect(url, handle_bitfinex, producer, symbols)

async def handle_bitfinex(ws, producer, symbols):
    for symbol in symbols:
        subscribe_message = {
            "event": "subscribe",
            "channel": "book",
            "symbol": symbol,
            "prec": "R0",
            "len": "250",
        }
        await ws.send(json.dumps(subscribe_message))

        del subscribe_message["len"]
        subscribe_message["channel"] = "trades"
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseBitfinex().normalise)

if __name__ == "__main__":
    asyncio.run(main())