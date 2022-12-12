from l3_atom.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from l3_atom.stream_processing.records import DexTrade
from yapic import json
import logging
from decimal import Decimal


# Also handles Sushiswap
class UniswapV2Handler(EthereumLogHandler):
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'

class UniswapV2SwapHandler(UniswapV2Handler):
    topic0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    event_name = "Swap"
    abi_name = 'uniswap_v2_pair'
    example_contract = '0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.uni_pool_data = json.loads(open('static/lists/uniswap_v2_pairs.json').read())
        self.sushi_pool_data = json.loads(open('static/lists/sushiswap_pairs.json').read())
        self.loaded_pool_data = True

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
        if poolAddr in self.uni_pool_data:
            exchange = 'uniswap-v2'
            pairDetails = self.uni_pool_data[poolAddr]
        elif poolAddr in self.sushi_pool_data:
            exchange = 'sushiswap'
            pairDetails = self.sushi_pool_data[poolAddr]
        else:
            logging.warning(f'Uniswap V2 / Sushiswap pair {poolAddr} not found in list')
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        if args.amount0In > 0:
            amountSold = args.amount0In
            tokenSold = token0['symbol']
            tokenSoldAddr = token0['id']
            amountBought = args.amount1Out
            tokenBought = token1['symbol']
            tokenBoughtAddr = token1['id']
        else:
            amountSold = args.amount1In
            tokenSold = token1['symbol']
            tokenSoldAddr = token1['id']
            amountBought = args.amount0Out
            tokenBought = token0['symbol']
            tokenBoughtAddr = token0['id']

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
            exchange=exchange,
            amountBought = Decimal(amountBought),
            amountSold = Decimal(amountSold)
        )
        
        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)