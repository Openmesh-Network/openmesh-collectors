from concurrent.futures import process
import dotenv
import websockets
import asyncio
import json
import requests
from web3 import Web3
from hexbytes import HexBytes
import time
from sink_connector.redis_producer import RedisProducer
import logging
import sys

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"

class Web3JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        if obj.__class__.__name__ == "AttributeDict":
            return obj.__dict__
        return super().default(obj)

def get_top_100_pairs():
    query = """
    {
      pairs(first: 100, orderBy: reserveUSD, orderDirection: desc) {
        id
        token0 {
            name
            symbol
        }
        token1 {
            name
            symbol
        }
      }
    }
    """
    pairs = {}
    res = requests.post(url=graph_endpoint, json={"query": query})
    result = res.json()
    for pair in result['data']['pairs']:
        pairs[pair['id']] = {
            'token0Name': pair['token0']['name'],
            'token1Name': pair['token1']['name'],
            'token0Symbol': pair['token0']['symbol'],
            'token1Symbol': pair['token1']['symbol']
        }
    return pairs

def convert_list_to_hexbytes(list_):
    for i in range(len(list_)):
        if isinstance(list_[i], str):
            list_[i] = HexBytes(list_[i])
        elif isinstance(list_[i], list):
            convert_list_to_hexbytes(list_[i])
        elif isinstance(list_[i], dict):
            convert_dict_to_hexbytes(list_[i])
    return list_

def convert_dict_to_hexbytes(dict_):
    for key in dict_:
        if isinstance(dict_[key], str):
            dict_[key] = HexBytes(dict_[key])
        elif isinstance(dict_[key], dict):
            convert_dict_to_hexbytes(dict_[key])
        elif isinstance(dict_[key], list):
            convert_list_to_hexbytes(dict_[key])
    return dict_

def process_swap_log(contract, hex_log):
    processed_log = contract.events.Swap().processLog(hex_log)
    return processed_log

def process_mint_log(contract, hex_log):
    processed_log = contract.events.Mint().processLog(hex_log)
    return processed_log

def process_burn_log(contract, hex_log):
    processed_log = contract.events.Burn().processLog(hex_log)
    return processed_log

def process_pair_log(contract, log, topics, pairs):
    log_topic_0 = log['topics'][0]
    hex_log = convert_dict_to_hexbytes(log.copy())
    process_function = topics[log_topic_0]
    processed_log = process_function(contract, hex_log).__dict__
    event_pair_info = pairs[processed_log['address'].hex()]
    processed_log['blockNumber'] = int(processed_log['blockNumber'].hex(), 16)
    processed_log['logIndex'] = int(processed_log['logIndex'].hex(), 16)
    processed_log['transactionIndex'] = int(processed_log['transactionIndex'].hex(), 16)
    processed_log['type'] = processed_log.pop('event')
    processed_log['exchange'] = 'UniswapV2'
    processed_log['data'] = processed_log.pop('args').__dict__
    processed_log['data']['token0Name'] = event_pair_info['token0Name']
    processed_log['data']['token1Name'] = event_pair_info['token1Name']
    processed_log['data']['token0Symbol'] = event_pair_info['token0Symbol']
    processed_log['data']['token1Symbol'] = event_pair_info['token1Symbol']
    processed_log['processedTimestamp'] = int(time.time() * (10 ** 3))
    return processed_log

async def subscribe_to_logs(ws, pair, msg, topics):
    msg["params"][1]["address"] = pair
    for topic in topics.keys():
        msg["params"][1]["topics"] = [topic]
        await ws.send(json.dumps(msg))

async def handle_events():
    conf = dotenv.dotenv_values("./keys/.env")
    ws_endpoint = conf["INFURA_WS_ENDPOINT"]
    # Topics for Swaps, Mints, Burns, respectively. Maps to processing function
    topics = {
        "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": process_swap_log, "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": process_mint_log, "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": process_burn_log
    }
    contract_addr = "0xAE461cA67B15dc8dc81CE7615e0320dA1A9aB8D5"

    web3 = Web3(Web3.WebsocketProvider(ws_endpoint))

    contract_as_parser = web3.eth.contract(address=contract_addr, abi=json.load(open("ABIs/uniswap_pair_abi.json")))

    event_filter_sub_message = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_subscribe",
        "params": [
            "logs",
            {
                "address": "",
                "topics": [],
            }
        ]
    }

    pairs = get_top_100_pairs()

    producer = RedisProducer('uniswap-raw')

    async with websockets.connect(ws_endpoint) as infura_connection:
        tasks = []
        for pair in pairs.keys():
            tasks.append(subscribe_to_logs(infura_connection, pair, event_filter_sub_message.copy(), topics))
        await asyncio.gather(*tasks)
        for _ in range(len(pairs) * 3):
            msg = await infura_connection.recv()
            while "result" not in json.loads(msg):
                msg = await infura_connection.recv()
        logging.info("Finished Subscribing")
        while True:
            message = await infura_connection.recv()
            processed_log = process_pair_log(contract_as_parser, json.loads(message)['params']['result'], topics, pairs)
            await producer.produce(processed_log['processedTimestamp'], json.dumps(processed_log, cls=Web3JsonEncoder))

if __name__ == "__main__":
    asyncio.run(handle_events())