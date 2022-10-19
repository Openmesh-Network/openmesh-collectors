from .coinbase import Coinbase
from .binance import Binance
from .binance_futures import BinanceFutures
from .apollox import ApolloX
from .dydx import Dydx
from .bitfinex import Bitfinex
from .gemini import Gemini
from .deribit import Deribit
from .bybit import Bybit
from .ftx import FTX

mapping = {
    'coinbase': Coinbase,
    'binance': Binance,
    'binance-futures': BinanceFutures,
    'bitfinex': Bitfinex,
    'dydx': Dydx,
    'apollox': ApolloX,
    'gemini': Gemini,
    'deribit': Deribit,
    'bybit': Bybit,
    'ftx': FTX
}
