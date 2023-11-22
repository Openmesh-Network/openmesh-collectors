from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
from decimal import Decimal


class DodoexHandler(EthereumLogHandler):
    graph_endpoint: str = 'https://thegraph.com/hosted-service/subgraph/dodoex/dodoex-v2'
    exchange = 'dodoex'


class DodoexPairHandler(DodoexHandler):
    abi_name = 'dodoex_pair'
    example_contract = '0xC9f93163c99695c6526b799EbcA2207Fdf7D61aD'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.pool_data = json.loads(
            open('static/lists/dodo_pairs.json').read())
        self.load_erc20_data()
        self.loaded_pool_data = True


class DodoexSwapHandler(DodoexPairHandler):
    abi_name = 'dodoex_v2_pool'
    example_contract = '0x45738075e430ea625c9164f2C2A28f66864B42E9'

    topic0 = '0xc2c0245e056d5fb095f04cd6373bc770802ebd1e6c918eb78fdef843cdb37b0f'
    event_name = 'DODOSwap'

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event['args']
        poolAddr = event['address']
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            return
        tokenSoldAddr = args['toToken']
        tokenBoughtAddr = args['fromToken']
        tokenSold = pairDetails['baseToken']['symbol'] if tokenSoldAddr == pairDetails[
            'baseToken']['id'] else pairDetails['quoteToken']['symbol']
        tokenBought = pairDetails['baseToken']['symbol'] if tokenBoughtAddr == pairDetails[
            'baseToken']['id'] else pairDetails['quoteToken']['symbol']
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        amountSold = Decimal(args['toAmount']) / Decimal(10 ** tokenSoldDecimals)
        amountBought = Decimal(args['fromAmount']) / \
            Decimal(10 ** tokenBoughtDecimals)
        taker = args['receiver']
        maker = args['trader']

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
            maker=maker
        )

        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)


class DodoexBuyHandler(DodoexPairHandler):
    topic0 = "0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3"
    event_name = "BuyBaseToken"

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = ['args']
        poolAddr = event['address']
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            return
        baseToken = pairDetails['baseToken']
        quoteToken = pairDetails['quoteToken']
        tokenSold = quoteToken['symbol']
        tokenSoldAddr = quoteToken['id']
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        amountSold = Decimal(args['payQuote']) / Decimal(10 ** tokenSoldDecimals)
        tokenBought = baseToken['symbol']
        tokenBoughtAddr = baseToken['id']
        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        amountBought = Decimal(args['receiveBase']) / \
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


class DodoexSellHandler(DodoexPairHandler):
    topic0 = "0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d"
    event_name = "SellBaseToken"

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = ['args']
        poolAddr = event['address']
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            return
        baseToken = pairDetails['baseToken']
        quoteToken = pairDetails['quoteToken']
        tokenSold = baseToken['symbol']
        tokenSoldAddr = baseToken['id']
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        amountSold = Decimal(args['payBase']) / Decimal(10 ** tokenSoldDecimals)
        tokenBought = quoteToken['symbol']
        tokenBoughtAddr = quoteToken['id']
        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        amountBought = Decimal(args['receiveQuote']) / \
            Decimal(10 ** tokenBoughtDecimals)
        taker = args['seller']

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
