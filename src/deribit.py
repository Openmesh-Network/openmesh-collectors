import websockets
import asyncio
import time
import json

from normalise.deribit_normalisation import NormaliseDeribit
from helpers.read_config import get_symbols
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages
from source_connector.websocket_connector import connect

url = 'wss://www.deribit.com/ws/api/v2'

async def main():
    raw_producer = KafkaProducer("deribit-raw")
    normalised_producer = KafkaProducer("deribit-normalised")
    trades_producer = KafkaProducer("deribit-trades")
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