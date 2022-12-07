from l3_atom.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from l3_atom.stream_processing.records import DexTrade
from yapic import json
import logging
from decimal import Decimal

class UniswapV3Handler(EthereumLogHandler):
    exchange = 'uniswap-v3'
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'

class UniswapV3SwapHandler(UniswapV3Handler):
    topic0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    event_name = "Swap"
    abi_name = 'uniswap_v3_pool'
    example_contract = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.pool_data = json.loads(open('static/lists/uniswap_v3_pools.json').read())
        self.loaded_pool_data = True

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            logging.warning(f'Uniswap V3 pool {poolAddr} not found in list')
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        if args['amount0'] > 0:
            tokenBought = token0['symbol']
            tokenBoughtAddr = token0['id']
            tokenSold = token1['symbol']
            tokenSoldAddr = token1['id']
            amountBought = args['amount0']
            amountSold = args['amount1']
        else:
            tokenBought = token1['symbol']
            tokenBoughtAddr = token1['id']
            tokenSold = token0['symbol']
            tokenSoldAddr = token0['id']
            amountBought = args['amount1']
            amountSold = args['amount0']

        msg = dict(
            blockNumber = event.blockNumber,
            blockHash = event.blockHash,
            transactionHash = event.transactionHash,
            logIndex = event.logIndex,
            pairAddr = poolAddr,
            tokenBought = tokenBought,
            tokenBoughtAddr = tokenBoughtAddr,
            tokenSold = tokenSold,
            tokenSoldAddr = tokenSoldAddr,
            blockTimestamp = blockTimestamp,
            atomTimestamp = atomTimestamp,
            exchange=self.exchange,
            amountBought = Decimal(amountBought),
            amountSold = Decimal(amountSold)
        )
        
        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)