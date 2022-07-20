import websockets
import asyncio
import time
import json

from normalise.deribit_normalisation import NormaliseDeribit
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://www.deribit.com/ws/api/v2'

async def main():
    raw_producer = RedisProducer("deribit-raw")
    normalised_producer = RedisProducer("deribit-normalised")
    trades_producer = RedisProducer("deribit-trades")
    symbols = get_symbols('deribit')
    await connect(url, handle_deribit, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_deribit(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": {
                "channels": [f"book.{symbol}.100ms", f"trades.{symbol}.100ms"]}
        }
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseDeribit().normalise)

if __name__ == "__main__":
    asyncio.run(main())