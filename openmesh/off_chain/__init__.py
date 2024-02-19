from .coinbase import Coinbase
from .binance import Binance
from .binance_futures import BinanceFutures
from .apollox import ApolloX
from .dydx import Dydx
from .bitfinex import Bitfinex
from .gemini import Gemini
from .deribit import Deribit
from .bybit import Bybit
from .kraken import Kraken
from .kraken_futures import KrakenFutures
from .opensea import OpenSea
from .phemex import Phemex


exch = [
    Coinbase,
    Binance,
    BinanceFutures,
    ApolloX,
    Dydx,
    Bitfinex,
    Gemini,
    Deribit,
    Bybit,
    Kraken,
    KrakenFutures,
    Phemex,
    OpenSea
]

mapping = {
    e.name: e for e in exch
}
