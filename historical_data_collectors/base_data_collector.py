import ccxt
from abc import ABC, abstractmethod

class BaseDataCollector(ABC):

    exchange = None
    @abstractmethod
    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        for symbol in self.symbols:

            #assuming we only need spot data
            if self.markets[symbol]['type'] == 'spot':
                print(symbol)
                self.fetch_and_write_symbol_trades(symbol, start_date, end_date)
                break


