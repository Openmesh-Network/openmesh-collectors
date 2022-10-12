from .coinbase import Coinbase
from .binance import Binance
from .binance_futures import BinanceFutures

mapping = {
    'coinbase': Coinbase,
    'binance': Binance,
    'binance-futures': BinanceFutures
}
