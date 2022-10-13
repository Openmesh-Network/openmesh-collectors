from .coinbase import Coinbase
from .binance import Binance
from .binance_futures import BinanceFutures
from .dydx import Dydx

mapping = {
    'coinbase': Coinbase,
    'binance': Binance,
    'binance-futures': BinanceFutures,
    'dydx': Dydx
}
