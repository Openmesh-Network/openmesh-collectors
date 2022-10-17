from .coinbase import Coinbase
from .binance import Binance
from .binance_futures import BinanceFutures
from .dydx import Dydx
from .bitfinex import Bitfinex

mapping = {
    'coinbase': Coinbase,
    'binance': Binance,
    'binance-futures': BinanceFutures,
    'bitfinex': Bitfinex,
    'dydx': Dydx
}
