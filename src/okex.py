import websockets
import asyncio
import time
import json

from normalise.okex_normalisation import NormaliseOkex
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_raw, enrich_lob_events, enrich_market_orders
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages

url = "wss://ws.okex.com:8443/ws/v5/public"

async def main():
    raw_producer = KafkaProducer("okex-raw")
    normalised_producer = KafkaProducer("okex-normalised")
    trades_producer = KafkaProducer("okex-trades")
    symbols = get_symbols('okex')
    async for ws in websockets.connect(url):
        try:
            t0 = time.time()
            # Subscribe
            for symbol in symbols:
                subscribe_message = {}
                subscribe_message['op'] = 'subscribe'
                subscribe_message['args'] = [{"channel": "books", "instId": symbol}]
                await ws.send(json.dumps(subscribe_message))
                subscribe_message['args'] = [{"channel": "trades", "instId": symbol}]
                await ws.send(json.dumps(subscribe_message))
            
            await produce_messages(ws, raw_producer, normalised_producer, trades_producer, NormaliseOkex().normalise)
        except websockets.ConnectionClosedError:
            t1 = time.time()
            print(f"{t1 - t0} seconds elasped before disconnection")
            continue

if __name__ == "__main__":
    asyncio.run(main())