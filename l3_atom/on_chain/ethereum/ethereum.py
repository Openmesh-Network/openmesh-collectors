import json
import time
import aiohttp
import websockets
import asyncio
import dotenv
from hexbytes import HexBytes
from sink_connector.redis_producer import RedisProducer
from helpers.normalise_transaction import normalise_transaction
from helpers.normalise_block import normalise_block
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='[%(asctime)s] - %(levelname)s - %(message)s',
)

logging.info("Starting collector")

async def produce_transactions(block_object, block_msg, redis_producer):
    pipe = redis_producer.pool.pipeline()
    for transaction in block_object['transactions']:
        new_tx = normalise_transaction(transaction, block_msg)
        pipe.xadd('ethereum-raw', fields={new_tx['tx_hash']: json.dumps(new_tx)}, maxlen=redis_producer.stream_max_len, approximate=True)
    await pipe.execute()

def serialize_transaction(transaction):
    """
    Helper for serializing a transaction to a dictionary
    """
    return json.loads(json.dumps(transaction, default=vars))

class Web3JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        if obj.__class__.__name__ == "AttributeDict":
            return obj.__dict__
        return super().default(obj)

async def get_all_transactions(conf, producer):
    session = aiohttp.ClientSession()
    try:
        async with websockets.connect(conf["INFURA_WS_ENDPOINT"]) as ws:
            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": ["newHeads"]}))
            await ws.recv()
            logging.info("Connected to websocket")
            old_num_int = -1
            request = {"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": ["", True]}
            while True:
                new_block = await ws.recv()
                new_num = json.loads(new_block)["params"]["result"]["number"]
                new_num_int = int(new_num, 16)
                if new_num_int == old_num_int:
                    logging.warning("Getting same block twice")
                    continue
                if old_num_int != -1 and new_num_int > old_num_int + 1:
                    logging.warning("One or more blocks have been skipped")
                request['params'][0] = new_num
                block_res = await session.post(conf["INFURA_REST_ENDPOINT"], json=request)
                block = json.loads(json.dumps(await block_res.json(), cls=Web3JsonEncoder))
                block_res.close()
                block = block['result']
                block_msg = normalise_block(block)
                await produce_transactions(block, block_msg, producer)
                logging.info("Produced block number: %s", str(new_num_int))
                old_num_int = new_num_int
    except KeyboardInterrupt:
        logging.info("Exiting by user request")
    finally:
        await session.close()

if __name__ == "__main__":
    producer = RedisProducer("ethereum-raw")
    conf = dotenv.dotenv_values('./keys/.env')
    asyncio.run(get_all_transactions(conf, producer))
