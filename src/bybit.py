import websockets
import asyncio
import time
import json

from normalise.bybit_normalisation import normalise
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_raw, enrich_lob_events, enrich_market_orders
from sink_connector.kafka_producer import KafkaProducer

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

            async for msg in ws:
                raw_producer.produce(str(time.time()), msg)

                msg = enrich_raw(json.loads(msg))
                lob_events, market_orders = normalise(msg)
                enrich_lob_events(lob_events)
                enrich_market_orders(market_orders)

                for event in lob_events:
                    normalised_producer.produce(str(time.time()), event)
                for trade in market_orders:
                    trades_producer.produce(str(time.time()), trade)

        except websockets.ConnectionClosedError:
            t1 = time.time()
            print(f"{t1 - t0} seconds elasped before disconnection")
            continue

if __name__ == "__main__":
    asyncio.run(main())