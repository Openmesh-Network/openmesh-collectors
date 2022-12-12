from l3_atom.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from l3_atom.stream_processing.records import DexTrade
from yapic import json
import logging
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
        self.pool_data = json.loads(open('static/lists/dodo_pairs.json').read())
        self.loaded_pool_data = True

class DodoexBuyHandler(DodoexPairHandler):
    topic0 = "0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3"
    event_name = "BuyBaseToken"

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            logging.warning(f'Dodoex {poolAddr} not found in list')
            return
        baseToken = pairDetails['baseToken']
        quoteToken = pairDetails['quoteToken']
        amountSold = args.payQuote
        tokenSold = quoteToken['symbol']
        tokenSoldAddr = quoteToken['id']
        amountBought = args.receiveBase
        tokenBought = baseToken['symbol']
        tokenBoughtAddr = baseToken['id']

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

class DodoexSellHandler(DodoexPairHandler):
    topic0 = "0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d"
    event_name = "SellBaseToken"

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            logging.warning(f'Dodoex {poolAddr} not found in list')
            return
        baseToken = pairDetails['baseToken']
        quoteToken = pairDetails['quoteToken']
        amountSold = args.payBase
        tokenSold = baseToken['symbol']
        tokenSoldAddr = baseToken['id']
        amountBought = args.receiveQuote
        tokenBought = quoteToken['symbol']
        tokenBoughtAddr = quoteToken['id']

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