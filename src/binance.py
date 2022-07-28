import websockets
import asyncio
import time
import json

from normalise.binance_normalisation import NormaliseBinance
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect
from source_connector.restapi_calls import get_snapshot

book_url = "wss://stream.binance.com:9443/ws"
snapshot_url = "https://api.binance.com/api/v3/depth"

async def main():
    producer = RedisProducer("binance")
    symbols = get_symbols('binance')
    await connect(book_url, handle_binance, producer, symbols, True)

async def handle_binance(ws, producer, symbols, is_book):
    normalise = NormaliseBinance().normalise
    for symbol in symbols:
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [
                symbol.lower() + "@trade",
                symbol.lower() + "@depth@100ms"
            ],
            "id": 1
        }
        await ws.send(json.dumps(subscribe_message))
        snapshot = await get_snapshot(snapshot_url + "?symbol=" + symbol.upper() + "&limit=5000")
        await produce_message(snapshot, producer, normalise)
    
    await produce_messages(ws, producer, normalise)

if __name__ == "__main__":
    asyncio.run(main())