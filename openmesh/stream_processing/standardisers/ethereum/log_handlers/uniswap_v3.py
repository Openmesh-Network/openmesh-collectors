from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
from decimal import Decimal

class UniswapV3Handler(EthereumLogHandler):
    exchange = 'uniswap-v3'
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'


class UniswapV3PoolHandler(UniswapV3Handler):

    abi_name = 'uniswap_v3_pool'
    example_contract = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.pool_data = json.loads(
            open('static/lists/uniswap_v3_pools.json').read())
        self.load_erc20_data()
        self.loaded_pool_data = True

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        """Override this method to handle the event"""
        pass

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        await self.uniswap_callback(event, blockTimestamp, atomTimestamp)

class UniswapV3LiquidityHandler(UniswapV3PoolHandler):

    # Override this in the child class for burns and mints
    liquidity_event_type = NotImplemented

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        args = event['args']
        poolAddr = event['address']
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
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
            exchange=self.exchange,
            owner=args['owner']
        )

        await self.standardiser.send_to_topic('dex_liquidity', key_field='pairAddr', **msg)

class UniswapV3MintHandler(UniswapV3LiquidityHandler):
    topic0='0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde'
    event_name = 'Mint'
    liquidity_event_type = 'add'
    

class UniswapV3BurnHandler(UniswapV3LiquidityHandler):
    topic0 = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
    event_name = "Burn"
    liquidity_event_type = 'remove'


class UniswapV3SwapHandler(UniswapV3PoolHandler):
    topic0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    event_name = "Swap"

    async def uniswap_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        args = event['args']
        poolAddr = event['address']
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        token0Sym = token0['symbol']
        token1Sym = token1['symbol']
        token0Id = token0['id']
        token1Id = token1['id']
        if args['amount0'] > 0:
            tokenBought = token0Sym
            tokenBoughtAddr = token0Id
            tokenSold = token1Sym
            tokenSoldAddr = token1Id
            amountBought = args['amount0']
            amountSold = abs(args['amount1'])
        else:
            tokenBought = token1Sym
            tokenBoughtAddr = token1Id
            tokenSold = token0Sym
            tokenSoldAddr = token0Id
            amountBought = args['amount1']
            amountSold = abs(args['amount0'])

        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        amountBought = Decimal(amountBought) / \
            Decimal(10 ** tokenBoughtDecimals)
        amountSold = Decimal(amountSold) / Decimal(10 ** tokenSoldDecimals)
        taker = args['recipient']

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
            exchange=self.exchange,
            amountBought=amountBought,
            amountSold=amountSold,
            taker=taker,
        )

        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)
