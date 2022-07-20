import websockets
import asyncio
import aiohttp
import time
import json

from dotenv import dotenv_values
from email.mime import base
import hashlib
import base64
import hmac

from normalise.kucoin_normalisation import NormaliseKucoin
from helpers.read_config import get_symbols
from helpers.enrich_data import enrich_raw
# from sink_connector.kafka_producer import KafkaProducer
# from sink_connector.ws_to_kafka import produce_messages, produce_message
from sink_connector.redis_producer import RedisProducer
from sink_connector.ws_to_redis import produce_messages, produce_message
from source_connector.websocket_connector import connect

setup_data_url = "https://api.kucoin.com/api/v1/bullet-public" 
ENV_PATH = "./keys/.env"

async def main():
    raw_producer = RedisProducer("kucoin-raw")
    normalised_producer = RedisProducer("kucoin-normalised")
    trades_producer = RedisProducer("kucoin-trades")
    symbols = get_symbols('kucoin')
    normalise = NormaliseKucoin().normalise
    async with aiohttp.ClientSession() as session:
        async with session.post(setup_data_url) as resp:
            setup_data = json.loads(await resp.text())
    token = setup_data['data']['token']
    endpoint = setup_data['data']["instanceServers"][0]["endpoint"]
    url = f"{endpoint}?token={token}&[connectId=1545910660739]"    
    await connect(url, handle_kucoin, raw_producer, normalised_producer, trades_producer, normalise, symbols)

async def handle_kucoin(ws, raw_producer, normalised_producer, trades_producer, normalise, symbols):
    for symbol in symbols:
        subscribe_message = {
            'type': 'subscribe', 
            'topic': f'/market/level2:{symbol}', 
            'id': 1545910660739,
            "privateChannel": False,
            "response": True
        }
        await ws.send(json.dumps(subscribe_message))
        subscribe_message['topic'] = f'/market/match:{symbol}'
        await ws.send(json.dumps(subscribe_message))
        snapshot = await get_kucoin_snapshot(symbol)
        while 'msg' in json.loads(snapshot).keys():
            print(f"Snapshot returned error {json.loads(snapshot)['msg']}")
            snapshot = await get_kucoin_snapshot(symbol)
        # Snapshot is too large to produce as raw message
        # await produce_message(snapshot, raw_producer, normalised_producer, trades_producer, normalise)
        normalise(enrich_raw(json.loads(snapshot))) # Activate normaliser by passing in snapshot
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise)

async def get_kucoin_snapshot(symbol):
    # TODO: CAN GET LEVEL 3 DATA FROM KUCOIN VIA REST API. EXPLORE FURTHER
    api_info = dotenv_values(ENV_PATH)
    timestamp = int(time.time() * 1000)
    str_to_sign = str(timestamp) + "GET" + "/api/v3/market/orderbook/level2?symbol=" + symbol

    signature = base64.b64encode(hmac.new(
            api_info['API_SECRET'].encode('utf-8'), 
            str_to_sign.encode('utf-8'), 
            hashlib.sha256
        ).digest()
    ).decode('utf-8')
    passphrase = base64.b64encode(hmac.new(
            api_info['API_SECRET'].encode('utf-8'), 
            api_info['API_PASSPHRASE'].encode('utf-8'), 
            hashlib.sha256
        ).digest()
    ).decode('utf-8')

    snapshot_url = f'https://api.kucoin.com/api/v3/market/orderbook/level2?symbol={symbol}'
    headers = {
        "KC-API-KEY": api_info["API_KEY"],
        "KC-API-PASSPHRASE": passphrase,
        "KC-API-TIMESTAMP": str(int(time.time() * 1000)),
        "KC-API-KEY-VERSION": "2",
        "KC-API-SIGN": signature
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(snapshot_url) as resp:
            return await resp.text()

if __name__ == "__main__":
    asyncio.run(main())