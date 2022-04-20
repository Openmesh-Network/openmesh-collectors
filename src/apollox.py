import websockets
import asyncio
import aiohttp
import time
import json

from normalise.apollox_normalisation import NormaliseApolloX
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages
from source_connector.websocket_connector import connect

url = "wss://fstream.apollox.finance/ws/"
snapshot_url = "https://fapi.apollox.finance/fapi/v1/depth"

async def main():
    raw_producer = KafkaProducer("apollox-raw")
    normalised_producer = KafkaProducer("apollox-normalised")
    trades_producer = KafkaProducer("apollox-trades")
    symbols = get_symbols('apollox')
    normalise = NormaliseApolloX().normalise
    await connect(url, handle_apollox, raw_producer, normalised_producer, trades_producer, normalise, symbols)

async def handle_apollox(ws, raw_producer, normalised_producer, trades_producer, normalise, symbols):
    for symbol in symbols:
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [
                symbol.lower() + "@aggTrade",
                symbol.lower() + "@depth@100ms"
            ],
            "id": 1
        }
        await ws.send(json.dumps(subscribe_message))
        await produce_snapshot(symbol, raw_producer, normalised_producer, normalise)
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise)

async def produce_snapshot(symbol, raw_producer, normalised_producer, normalise):
    async with aiohttp.ClientSession() as session:
        async with session.get(snapshot_url, params={'symbol': symbol.upper()}) as resp:
            snapshot = await resp.text()
            raw_producer.produce(str(time.time()), snapshot)

            enriched = enrich_raw(json.loads(snapshot))
            lob_events, market_orders = normalise(enriched)
            enrich_lob_events(lob_events)
            enrich_market_orders(market_orders)

            for event in lob_events:
                normalised_producer.produce(str(time.time()), event)

if __name__ == "__main__":
    asyncio.run(main())