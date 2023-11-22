from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from decimal import Decimal


class HashflowHandler(EthereumLogHandler):
    exchange = 'hashflow'


class HashflowTradeHandler(HashflowHandler):
    topic0 = "0x8cf3dec1929508e5677d7db003124e74802bfba7250a572205a9986d86ca9f1e"
    event_name = "Trade"
    abi_name = 'hashflow_pool'
    example_contract = '0x5AFE266aB4E43c32baD5459FE8DF116Dd5541222'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loaded_pool_data = False

    def _load_pool_data(self):
        self.load_erc20_data()
        self.loaded_pool_data = True

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event['args']
        poolAddr = event['address']
        tokenSoldAddr = args['baseToken']
        tokenSold = self.get_symbol(tokenSoldAddr)
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        tokenBoughtAddr = args['quoteToken']
        tokenBought = self.get_symbol(tokenBoughtAddr)
        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)

        amountSold = Decimal(args['baseTokenAmount']) / \
            Decimal(10 ** tokenSoldDecimals)
        amountBought = Decimal(args['quoteTokenAmount']) / \
            Decimal(10 ** tokenBoughtDecimals)

        taker = args['trader']

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
