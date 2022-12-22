from l3_atom.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
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
        self.pool_data = json.loads(
            open('static/lists/uniswap_v3_pools.json').read())
        self.load_erc20_data()
        self.loaded_pool_data = True

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
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
        taker = args.recipient

        msg = dict(
            blockNumber=event.blockNumber,
            blockHash=event.blockHash,
            transactionHash=event.transactionHash,
            logIndex=event.logIndex,
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
