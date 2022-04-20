import websockets
import asyncio
import time
import json
from gzip import decompress

from normalise.huobi_normalisation import NormaliseHuobi
from helpers.read_config import get_symbols
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages
from source_connector.websocket_connector import connect

book_url = "wss://api-aws.huobi.pro/feed"
trades_url = 'wss://api-aws.huobi.pro/ws'

async def main():
    raw_producer = KafkaProducer("huobi-raw")
    normalised_producer = KafkaProducer("huobi-normalised")
    trades_producer = KafkaProducer("huobi-trades")
    symbols = get_symbols('huobi')
    await asyncio.gather(
        connect(book_url, handle_huobi, raw_producer, normalised_producer, trades_producer, symbols, True),
        connect(trades_url, handle_huobi, raw_producer, normalised_producer, trades_producer, symbols, False),
    )

async def handle_huobi(ws, raw_producer, normalised_producer, trades_producer, symbols, is_book):
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
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseHuobi().normalise)

if __name__ == "__main__":
    asyncio.run(main())