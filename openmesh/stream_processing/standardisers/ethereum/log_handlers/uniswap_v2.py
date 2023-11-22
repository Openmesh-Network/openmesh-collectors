from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
from decimal import Decimal


# Also handles Sushiswap
class UniswapV2Handler(EthereumLogHandler):
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'


class UniswapV2PairHandler(UniswapV2Handler):
    abi_name = 'uniswap_v2_pair'
    example_contract = '0xB4e16d0168e52d35CaCD2c6185b44281Ec28C9Dc'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.uni_pool_data = json.loads(
            open('static/lists/uniswap_v2_pairs.json').read())
        self.sushi_pool_data = json.loads(
            open('static/lists/sushiswap_pairs.json').read())
        self.load_erc20_data()
        self.loaded_pool_data = True

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        """Override this method to handle the event"""
        pass

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        await self.uniswap_callback(event, blockTimestamp, atomTimestamp)


class UniswapV2LiquidityHandler(UniswapV2PairHandler):

    # Override this in the child class for burns and mints
    liquidity_event_type = NotImplemented

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        args = event['args']
        poolAddr = event['address']
        if poolAddr in self.uni_pool_data:
            exchange = 'uniswap-v2'
            pairDetails = self.uni_pool_data[poolAddr]
        elif poolAddr in self.sushi_pool_data:
            exchange = 'sushiswap'
            pairDetails = self.sushi_pool_data[poolAddr]
        else:
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        token0Sym = token0['symbol']
        token1Sym = token1['symbol']
        token0Id = token0['id']
        token1Id = token1['id']
        
        rawAmount0 = args['amount0']
        rawAmount1 = args['amount1']

        token0Decimals = self.get_decimals(token0Id)
        token1Decimals = self.get_decimals(token1Id)
        amount0 = Decimal(rawAmount0) / \
            Decimal(10 ** token0Decimals)
        amount1 = Decimal(rawAmount1) / Decimal(10 ** token1Decimals)

        msg = dict(
            blockNumber=event['blockNumber'],
            blockHash=event['blockHash'],
            transactionHash=event['transactionHash'],
            logIndex=event['logIndex'],
            pairAddr=poolAddr,
            token0=token0Sym,
            token0Addr=token0Id,
            token1=token1Sym,
            token1Addr=token1Id,
            amount0=amount0,
            amount1=amount1,
            blockTimestamp=blockTimestamp,
            atomTimestamp=atomTimestamp,
            eventType=self.liquidity_event_type,
            exchange=exchange
        )

        await self.standardiser.send_to_topic('dex_liquidity', key_field='pairAddr', **msg)

class UniswapV2MintHandler(UniswapV2LiquidityHandler):
    topic0='0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
    event_name = 'Mint'
    liquidity_event_type = 'add'
    

class UniswapV2BurnHandler(UniswapV2LiquidityHandler):
    topic0 = "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496"
    event_name = "Burn"
    liquidity_event_type = 'remove'

class UniswapV2SwapHandler(UniswapV2PairHandler):
    topic0 = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
    event_name = "Swap"

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        args = event['args']
        poolAddr = event['address']
        if poolAddr in self.uni_pool_data:
            exchange = 'uniswap-v2'
            pairDetails = self.uni_pool_data[poolAddr]
        elif poolAddr in self.sushi_pool_data:
            exchange = 'sushiswap'
            pairDetails = self.sushi_pool_data[poolAddr]
        else:
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        if args['amount0In'] > 0:
            amountSold = args['amount0In']
            tokenSold = token0['symbol']
            tokenSoldAddr = token0['id']
            amountBought = args['amount1Out']
            tokenBought = token1['symbol']
            tokenBoughtAddr = token1['id']
        else:
            amountSold = args['amount1In']
            tokenSold = token1['symbol']
            tokenSoldAddr = token1['id']
            amountBought = args['amount0Out']
            tokenBought = token0['symbol']
            tokenBoughtAddr = token0['id']

        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        amountBought = Decimal(amountBought) / Decimal(10**tokenBoughtDecimals)
        amountSold = Decimal(amountSold) / Decimal(10**tokenSoldDecimals)
        taker = args['to']

        msg = dict(
            blockNumber=event['blockNumber'],
            blockHash=event['blockHash'],
            transactionHash=event['transactionHash'],
            logIndex=event['logIndex'],
            pairAddr=poolAddr,
            tokenBought=tokenBought,
            tokenBoughtAddr=tokenBoughtAddr,
            tokenSold=tokenSold,
            tokenSoldAddr=tokenSoldAddr,
            blockTimestamp=blockTimestamp,
            atomTimestamp=atomTimestamp,
            exchange=exchange,
            amountBought=amountBought,
            amountSold=amountSold,
            taker=taker
        )

        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)
