from .coinbase import CoinbaseStandardiser
from .binance import BinanceStandardiser
from .binance_futures import BinanceFuturesStandardiser
from .dydx import DydxStandardiser
from .bitfinex import BitfinexStandardiser
from .apollox import ApolloXStandardiser

standardisers = [CoinbaseStandardiser,
                 BinanceStandardiser, BinanceFuturesStandardiser, BitfinexStandardiser, DydxStandardiser, ApolloXStandardiser]
