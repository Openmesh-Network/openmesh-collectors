import ccxt
import datetime
import pytz
# from historical_data_collectors.collectors.base_data_collector import BaseDataCollector
from .base_data_collector import BaseDataCollector
from ..helpers.profiler import Profiler
import time

#How many seconds to sleep in case we get rate limited
RATE_LIMIT_SLEEP_TIME = 0.5

#rate limit to set ccxt's rate limit to. This is in milliseconds. ccxt will send requests no earlier than RATE_LIMIT millseconds
#According to coinbase documentation. This should be set to 100. But the calls are getting rate limited even at 150, so going with that
RATE_LIMIT = 150


class CoinbaseDataCollector(BaseDataCollector):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        super().__init__()
        self.exchange = ccxt.coinbase()
        self.exchange.rateLimit = RATE_LIMIT
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""
        super().fetch_and_write_trades(start_date, end_date)

    def fetch_and_write_symbol_trades(self, symbol, start_time, end_time):
        """Fetches and writes the l2 trades for the given symbol and inserts it into the database"""

        #coinbase fetched the latest trades before 'until' first and then paginates backwards, so we have to iterate the end_time
        while start_time < end_time:
            
            try:
                #fetches the most recent trades no later than 'until' and no earier than 'since'
                self.profiler.start('fetching call')
                trades = self.exchange.fetch_trades(symbol, since = start_time, limit = 1000, params = {"until": end_time})
                self.profiler.stop('fetching call')

                print(self.exchange.iso8601(start_time), len(trades), 'trades')
                print(self.exchange.iso8601(end_time), len(trades), 'trades')
                
                #we fetched new trades, keep fetching
                if len(trades):

                    first_trade = trades[0]
                    end_time = first_trade['timestamp']

                    # The until timestamp in the fetch_trades call is exclusive, so we don't fetch trades at 'until' timestamp
                    # This means that all the trades we fetch in a new call are new, we can just write them all to the database
                    # without having to filter out previously fetched trades like we have to with binance

                    # write to database
                    l2_trades = super().normalize_to_l2(trades, 'Coinbase')
                    self.write_to_database(l2_trades)

                # no more new trades left to be fetched since 'since' timestamp
                else:
                    end_time = start_time - 1

                print("--FETCHED---")
                print(len(trades), "trades")

                if len(trades):
                    print(trades[0])
                    print("-----")
                    print(trades[-1])

            #If we get rate limited, pause for RATE_LIMIT_SLEEP_TIME before trying again
            except (ccxt.NetworkError, ccxt.BaseError) as e:
                print(type(e).__name__, str(e))
                time.sleep(RATE_LIMIT_SLEEP_TIME)