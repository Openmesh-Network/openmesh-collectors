from .coinbase import CoinbaseStandardiser
from .binance import BinanceStandardiser
from .binance_futures import BinanceFuturesStandardiser
from .dydx import DydxStandardiser
from .bitfinex import BitfinexStandardiser
from .apollox import ApolloXStandardiser
from .gemini import GeminiStandardiser
from .deribit import DeribitStandardiser
from .bybit import BybitStandardiser
from .ftx import FTXStandardiser
from .kraken import KrakenStandardiser
from .kraken_futures import KrakenFuturesStandardiser
from .phemex import PhemexStandardiser
from .ethereum import EthereumStandardiser

standardisers = [CoinbaseStandardiser,
                 BinanceStandardiser, BinanceFuturesStandardiser, BitfinexStandardiser, DydxStandardiser, ApolloXStandardiser, GeminiStandardiser, DeribitStandardiser, BybitStandardiser, FTXStandardiser, KrakenStandardiser, KrakenFuturesStandardiser, PhemexStandardiser, EthereumStandardiser]
