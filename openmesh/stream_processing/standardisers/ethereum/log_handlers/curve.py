from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
from decimal import Decimal


class CurveHandler(EthereumLogHandler):
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/blocklytics/curve'
    exchange = 'curve'


class CurveSwapHandler(CurveHandler):
    topic0 = "0x8b3e96f2b889fa771c53c981b40daf005f63f637f1869f707052d15a3dd97140"
    event_name = "TokenExchange"
    abi_name = 'curve_pool'
    example_contract = '0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.pool_data = json.loads(
            open('static/lists/curve_pools.json').read())
        self.load_erc20_data()
        self.loaded_pool_data = True

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event['args']
        poolAddr = event['address']
        tokens = self.pool_data.get(poolAddr, None)
        if tokens is None:
            return
        tokenSold = tokens[args['sold_id']]['symbol']
        tokenSoldAddr = tokens[args['sold_id']]['id']
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        tokenBought = tokens[args['bought_id']]['symbol']
        tokenBoughtAddr = tokens[args['bought_id']]['id']
        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)

        amountSold = Decimal(args['tokens_sold']) / \
            Decimal(10 ** tokenSoldDecimals)
        amountBought = Decimal(args['tokens_bought']) / \
            Decimal(10 ** tokenBoughtDecimals)

        taker = args['buyer']

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
            taker=taker
        )

        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)
