from .binance_futures import BinanceFuturesStandardiser
from openmesh.off_chain import ApolloX


class ApolloXStandardiser(BinanceFuturesStandardiser):
    exchange = ApolloX
