from .binance_futures import BinanceFuturesStandardiser
from l3_atom.off_chain import ApolloX


class ApolloXStandardiser(BinanceFuturesStandardiser):
    exchange = ApolloX
