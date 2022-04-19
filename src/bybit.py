import websockets
import asyncio
import time
import json

from normalise.bybit_normalisation import normalise
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_raw, enrich_lob_events, enrich_market_orders
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages

url = 'wss://stream.bybit.com/realtime'

async def main():
    raw_producer = KafkaProducer("bybit-raw")
    normalised_producer = KafkaProducer("bybit-normalised")
    trades_producer = KafkaProducer("bybit-trades")
    symbols = get_symbols('bybit')
    async for ws in websockets.connect(url):
        try:
            t0 = time.time()
            # Subscribe
            for symbol in symbols:
                subscribe_message = {
                        "op": "subscribe",
                        "args": ["orderBook_200.100ms." + symbol, "trade." + symbol]
                    }
                await ws.send(json.dumps(subscribe_message).encode('utf-8'))
            
            await produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise)
        except websockets.ConnectionClosedError:
            t1 = time.time()
            print(f"{t1 - t0} seconds elasped before disconnection")
            continue

if __name__ == "__main__":
    asyncio.run(main())