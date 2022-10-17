from .coinbase import CoinbaseStandardiser
from .binance import BinanceStandardiser
from .binance_futures import BinanceFuturesStandardiser
from .dydx import DydxStandardiser
from .bitfinex import BitfinexStandardiser

standardisers = [CoinbaseStandardiser,
                 BinanceStandardiser, BinanceFuturesStandardiser, BitfinexStandardiser, DydxStandardiser]
