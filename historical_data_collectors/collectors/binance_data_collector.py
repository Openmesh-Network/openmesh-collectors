import ccxt
import datetime
import pytz
from .base_data_collector import BaseDataCollector

ONE_HOUR_IN_SECONDS = 3600
ONE_HOUR_IN_MILLISECONDS = ONE_HOUR_IN_SECONDS * 1000
ONE_SECOND_IN_MILLISECONDS = 1000
MAX_BINANCE_API_LIMIT = 1000

class BinanceDataCollector(BaseDataCollector):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        super().__init__()
        self.exchange = ccxt.binance()
        self.exchange.rateLimit = 250
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols


    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        super().fetch_and_write_trades(start_date, end_date)

        # in milliseconds
        utc_timezone = pytz.utc

        # start_time = int(
        #     datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)
        # end_time = int(
        #     datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)

        # self.fetch_and_write_symbol_trades('LPT/USDT', start_time, end_time)


    def fetch_and_write_symbol_trades(self, symbol, start_time, end_time):
        """Fetches and writes the l2 trades for the given symbol and inserts it into the database"""

        previous_trade_id = None

        while start_time < end_time:

            try:
                
                #log the time program takes to run between consecutive fetch_trades calls
                if (self.profiler.started('time bw calls')):
                    self.profiler.stop('time bw calls')

                # Binance api returns the lesser of the next 500 trades since start_time or all the trades in the hour
                # since start_time
                self.profiler.start('fetch_trades call')
                trades = self.exchange.fetch_trades(symbol, since=start_time, limit = MAX_BINANCE_API_LIMIT)
                self.profiler.stop('fetch_trades call')
                
                self.profiler.start('time between calls')

                # print(trades)
                print("--FETCHED---")
                print(len(trades), "trades")

                if len(trades):
                    print(trades[0])
                    print("-----")
                    print(trades[-1])


                if len(trades):

                    last_trade = trades[-1]

                    #checks if we have fetched any new trades in this call
                    if previous_trade_id != last_trade['id']:
                        
                        # print("previous_trade_id", previous_trade_id)

                        #filter out any trades we've already fetched/written to the db in a past api call
                        trades_to_write = self.filter_new_trades(trades, previous_trade_id)

                        l2_trades = super().normalize_to_l2(trades_to_write, 'Binance')

                        self.write_to_database(l2_trades)

                        start_time = last_trade['timestamp']
                        previous_trade_id = last_trade['id']


                    # only one trade happened in the one hour since start_time. We've already written that trade to database.
                    # increase start_time by an horu
                    else:
                        start_time += ONE_HOUR_IN_MILLISECONDS

                # no trades were made in the one hour since start_time. Fetch trades for the next hour
                else:
                    start_time += ONE_HOUR_IN_MILLISECONDS


            except ccxt.NetworkError as e:
                print(type(e).__name__, str(e))


    def filter_new_trades(self, trades, previous_trade_id):
        """Returns trades that have occured after the previous_trade_id trade. 
        Effectively this returns trades that we've fetched for the first time from binance, 
        removing trades we've fetched previously"""

        #if we haven't fetched any trades before, all trades are new
        if previous_trade_id == None:
            return trades

        #if we have fetched trades before, check all the trades we've fetched now to identify old ones
        for i in range(len(trades)):

            #We've fetched trades occuring before this previously
            if trades[i]['id'] == previous_trade_id:
                # print(len(trades))
                # print(len())
                return trades[i+1:]

        #All trades are new
        return trades