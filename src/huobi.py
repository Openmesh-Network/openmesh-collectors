import websockets
import asyncio
import time
import json
from gzip import decompress

from normalise.huobi_normalisation import NormaliseHuobi
from helpers.read_config import get_symbols
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

book_url = "wss://api-aws.huobi.pro/feed"
trades_url = 'wss://api-aws.huobi.pro/ws'

async def main():
    producer = RedisProducer("huobi")
    symbols = get_symbols('huobi')
    await asyncio.gather(
        connect(book_url, handle_huobi, producer, symbols, True),
        connect(trades_url, handle_huobi, producer, symbols, False),
    )

async def handle_huobi(ws, producer, symbols, is_book):
    for symbol in symbols:
        if is_book is True:
            subscribe_message = {'sub': f'market.{symbol}.mbp.400', 
                'id': 'id1'
            }
            await ws.send(json.dumps(subscribe_message))
            subscribe_message['req'] = subscribe_message.pop('sub')
            await ws.send(json.dumps(subscribe_message))
        else:
            subscribe_message = {'sub': f'market.{symbol}.trade.detail', 
                'id': 'id1'
            }
            await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, producer, NormaliseHuobi().normalise)

if __name__ == "__main__":
    asyncio.run(main())