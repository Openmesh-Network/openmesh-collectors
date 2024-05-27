import ccxt
import datetime
import pytz
# from historical_data_collectors.collectors.base_data_collector import BaseDataCollector
from .base_data_collector import BaseDataCollector
from ..helpers.profiler import Profiler
import time

RATE_LIMIT_SLEEP_TIME = 0.5

class OkxDataCollector(BaseDataCollector):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        super().__init__()
        self.exchange = ccxt.okx()
        self.exchange.rateLimit = 100
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols
        

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""
        
        super().fetch_and_write_trades(start_date, end_date)


    def fetch_and_write_symbol_trades(self, symbol, start_time, end_time):
        """Fetches and writes the l2 trades for the given symbol and inserts it into the database
           Okx api only supports fetching from the most recent trade and paginating backwards.
           It also only supports past 3 months of data"""

        #pagination parameter. Need to pass to okx api to get pages before this id
        after_id = None

        #params we pass to the okx api
        params = None

        # fetched the latest trades first and then paginates backwards, so we have to iterate the end_time
        while start_time < end_time:

            try:

                self.profiler.start('fetching call')
                
                #dont pass any pagination param for the first call
                if after_id is None:
                    trades = self.exchange.fetch_trades(symbol, params = {'method': 'publicGetMarketHistoryTrades'})
                
                #pass after_id pagination param 
                else:
                    params = {'after': after_id,
                              'method': 'publicGetMarketHistoryTrades'
                            }
                    trades = self.exchange.fetch_trades(symbol, params = params)

                self.profiler.stop('fetching call')

                print(self.exchange.iso8601(start_time), len(trades), 'trades')
                print(self.exchange.iso8601(end_time), len(trades), 'trades')

                #we fetched new trades, write these to db, update pagination param 
                if len(trades):
                    
                    first_trade = trades[0]

                    #we assign the id of the first trade as the pagination param. So next call will fetch pages containing
                    #trades before this trade_id
                    after_id = first_trade['id']
                    end_time = first_trade['timestamp']

                    # write to database
                    l2_trades = super().normalize_to_l2(trades, 'Okx')
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