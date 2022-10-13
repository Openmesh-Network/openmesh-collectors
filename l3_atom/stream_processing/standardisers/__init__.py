from .coinbase import CoinbaseStandardiser
from .binance import BinanceStandardiser
from .binance_futures import BinanceFuturesStandardiser
from .dydx import DydxStandardiser

standardisers = [CoinbaseStandardiser,
                 BinanceStandardiser, BinanceFuturesStandardiser, DydxStandardiser]
