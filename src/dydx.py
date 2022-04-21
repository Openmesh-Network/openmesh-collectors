import websockets
import asyncio
import time
import json

from normalise.dydx_normalisation import NormaliseDydx
from helpers.read_config import get_symbols
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages
from source_connector.websocket_connector import connect

url = 'wss://api.dydx.exchange/v3/ws'

async def main():
    raw_producer = KafkaProducer("dydx-raw")
    normalised_producer = KafkaProducer("dydx-normalised")
    trades_producer = KafkaProducer("dydx-trades")
    symbols = get_symbols('dydx')
    await connect(url, handle_dydx, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_dydx(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {'type': 'subscribe', 
                'channel': 'v3_orderbook', 
                'id': symbol,
                'includeOffsets': True
            }
        await ws.send(json.dumps(subscribe_message))
        subscribe_message['channel'] = 'v3_trades'
        del subscribe_message['includeOffsets']
        await ws.send(json.dumps(subscribe_message))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseDydx().normalise)

if __name__ == "__main__":
    asyncio.run(main())