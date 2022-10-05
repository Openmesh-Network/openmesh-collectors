import asyncio
import json
import requests
from sink_connector.redis_producer import RedisProducer
import aiohttp
import time

#response = json.load(open("./schemas/indicator_response_template.json"))
v2_graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
v3_graph_endpoint="https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"

QUERY_INTERVAL = 30

async def get_global_stats(session):
    request = \
    """{
            factory(id:"0x1F98431c8aD98523631AE4a59f267346ea31F984") {
                poolCount
                txCount
                totalVolumeUSD
                totalVolumeETH
                totalFeesUSD
                totalFeesETH
                totalValueLockedUSD
                totalValueLockedETH
            }
            
            pools(orderBy:totalValueLockedUSD, orderDirection: desc, first: 10) {
                token0 {
                    symbol
                    name
                }
                token1 {
                    symbol
                    name
                }
                id
                liquidity
                totalValueLockedToken0
                totalValueLockedToken1
                volumeToken0
                volumeToken1
                feesUSD
                volumeUSD
                totalValueLockedUSD
                token0Price
                token1Price
                txCount
            }
        }"""
    async with session.post(v3_graph_endpoint, json={"query": request}) as res:
            
        #res = requests.post(url=v3_graph_endpoint, json={"query": request})
        query_res = await res.json()
        #print(json.dumps(query_res, indent=4))
        result = query_res['data']
        result['timestamp'] = int(time.time() * 1000)
        result['meta'] = result.pop('factory')
        for pool in result['pools']:
            pool['token0']['volume'] = pool.pop('volumeToken0')
            pool['token1']['volume'] = pool.pop('volumeToken1')
            pool['token0']['totalValueLocked'] = pool.pop('totalValueLockedToken0')
            pool['token1']['totalValueLocked'] = pool.pop('totalValueLockedToken1')
            pool['token0']['price'] = pool.pop('token0Price')
            pool['token1']['price'] = pool.pop('token1Price')
            pool['contractHash'] = pool.pop('id')
        return result

async def collect_indicators():
    producer = RedisProducer('uniswap-indicators')
    async with aiohttp.ClientSession() as session:
        while True:
            res = await get_global_stats(session)
            await producer.produce(time.time(), json.dumps(res))
            await asyncio.sleep(QUERY_INTERVAL)

if __name__ == '__main__':
    asyncio.run(collect_indicators())