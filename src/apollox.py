import websockets
import asyncio
import time
import json

from normalise.apollox_normalisation import NormaliseApolloX
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect
from source_connector.restapi_calls import get_snapshot

url = "wss://fstream.apollox.finance/ws/"
snapshot_url = "https://fapi.apollox.finance/fapi/v1/depth"

async def main():
    producer = RedisProducer("apollox")
    symbols = get_symbols('apollox')
    normalise = NormaliseApolloX().normalise
    await connect(url, handle_apollox, producer, normalise, symbols)

async def handle_apollox(ws, producer, normalise, symbols):
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
        snapshot = await get_snapshot(snapshot_url + "?symbol=" + symbol.upper())
        await produce_message(snapshot, producer, normalise)
    
    await produce_messages(ws, producer, normalise)


if __name__ == "__main__":
    asyncio.run(main())