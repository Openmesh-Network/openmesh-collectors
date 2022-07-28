import websockets
import asyncio
import time
import json

from normalise.ftx_normalisation import NormaliseFtx
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

url = 'wss://ftx.com/ws/'

async def main():
    producer = RedisProducer("ftx")
    symbols = get_symbols('ftx')
    await connect(url, handle_ftx, producer, symbols)

async def handle_ftx(ws, producer, symbols):
    for symbol in symbols:
        subscribe_message = {
                'op': 'subscribe', 
                'channel': 'orderbook', 
                'market': symbol
            }
        await ws.send(json.dumps(subscribe_message))
        subscribe_message['channel'] = 'trades'
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseFtx().normalise)

if __name__ == "__main__":
    asyncio.run(main())