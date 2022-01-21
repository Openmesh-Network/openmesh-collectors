"""
normalising_strategies

Normalising strategies for each exchange. Takes input from data feeds and normalises them into 
a form suitable to be put into the data tables.
"""
from typing import Callable

from .ftx import NormaliseFtx
from .huobi import NormaliseHuobi
from .okex import NormaliseOkex
from .phemex import NormalisePhemex
from .kraken_futures import NormaliseKrakenFutures
from .kraken import NormaliseKraken
from .kucoin import NormaliseKucoin

class NormalisingStrategies():
    def __init__(self):
        self.strategies = {}
        self.register()

    def register(self):
        """Initialises the normalising strategies for the given exchanges"""
        self.strategies["kraken"] = NormaliseKraken()
        self.strategies["kraken-futures"] = NormaliseKrakenFutures()
        self.strategies["okex"] = NormaliseOkex()
        self.strategies["phemex"] = NormalisePhemex()
        self.strategies["huobi"] = NormaliseHuobi()
        self.strategies["ftx"] = NormaliseFtx()
        self.strategies["kucoin"] = NormaliseKucoin()

    def get_strategy(self, exchange_id) -> Callable:
        """Given an exchange id, returns the normalising strategy"""
        if not exchange_id in self.strategies.keys():
            raise KeyError(
                f"exchange id {exchange_id} not registered as a strategy")
        return self.strategies[exchange_id].normalise

