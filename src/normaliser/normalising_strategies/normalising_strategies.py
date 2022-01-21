"""
normalising_strategies

Normalising strategies for each exchange. Takes input from data feeds and normalises them into 
a form suitable to be put into the data tables.
"""
import time
import json
from typing import Callable

from .ftx import NormaliseFtx
from .huobi import NormaliseHuobi
from .okex import NormaliseOkex
from .phemex import NormalisePhemex
from .kraken import NormaliseKraken
from .kucoin import NormaliseKucoin

class NormalisingStrategies():
    def __init__(self):
        self.strategies = {}
        self.register()

    def register(self):
        self.strategies["kraken"] = NormaliseKraken()
        self.strategies["okex"] = NormaliseOkex()
        self.strategies["phemex"] = NormalisePhemex()
        self.strategies["huobi"] = NormaliseHuobi()
        self.strategies["ftx"] = NormaliseFtx()
        self.strategies["kucoin"] = NormaliseKucoin()

    def get_strategy(self, exchange_id) -> Callable:
        if not exchange_id in self.strategies.keys():
            raise KeyError(
                f"exchange id {exchange_id} not registered as a strategy")
        return self.strategies[exchange_id].normalise

