from .coinbase import CoinbaseStandardiser
from .binance import BinanceStandardiser
from .binance_futures import BinanceFuturesStandardiser

standardisers = [CoinbaseStandardiser, BinanceStandardiser, BinanceFuturesStandardiser]
