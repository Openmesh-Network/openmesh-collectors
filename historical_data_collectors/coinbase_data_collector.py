import ccxt
import datetime
import pytz
from historical_data_collectors.base_data_collector import BaseDataCollector
from .helpers.profiler import Profiler
import time

RATE_LIMIT_SLEEP_TIME = 0.5

class CoinbaseDataCollector(BaseDataCollector):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        self.exchange = ccxt.coinbase()
        self.exchange.rateLimit = 150
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols
        self.profiler = Profiler()

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""
        super().fetch_and_write_trades(start_date, end_date)

    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date, connection):
        """Fetches and writes the l2 trades for the given symbol and inserts it into the database"""

        utc_timezone = pytz.utc

        # in milliseconds
        start_time = int(
            datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)
        end_time = int(
            datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)

        one_hour = 3600 * 1000

        current_time = datetime.datetime.now()
        end_time = int(current_time.timestamp()*1000)

        two_hour_before = current_time - datetime.timedelta(hours=2)
        one_hour_before = current_time - datetime.timedelta(hours=1)
        five_min_before = current_time - datetime.timedelta(minutes=5)
        one_min_before = current_time - datetime.timedelta(minutes=1)
        one_second_before = current_time - datetime.timedelta(seconds=1)

        # start_time = int(one_second_before.timestamp() * 1000)
        # start_time = int(one_min_before.timestamp() * 1000)
        # start_time = int(five_min_before.timestamp() * 1000)
        start_time = int(one_hour_before.timestamp() * 1000)
        # start_time = int(two_hour_before.timestamp() * 1000)


        # count = 0

        #this is the id of the first trade we fetched in the last api call
        previous_trade_id = None

        #coinbase fetched the latest trades first and then paginates backwards, so we have to iterate the end_time
        while start_time < end_time:
        # while start_time < end_time and count < 3:

            try:

                # trades = self.exchange.fetch_trades(symbol, params = {"paginate": True})
                # print(start_time)
                # print(end_time)
                # trades = self.exchange.fetch_trades(symbol, since = start_time)
                # trades = self.exchange.fetch_trades(symbol, since = start_time, until = end_date)

                #fetches the most recent trades no later than 'until' and no earier than 'since'
                self.profiler.start('fetching call')
                trades = self.exchange.fetch_trades(symbol, since = start_time, limit = 1000, params = {"until": end_time})
                self.profiler.stop('fetching call')

                print(self.exchange.iso8601(start_time), len(trades), 'trades')
                print(self.exchange.iso8601(end_time), len(trades), 'trades')

                if len(trades):


                    first_trade = trades[0]
                    # if previous_trade_id != first_trade['id']:

                    end_time = first_trade['timestamp']
                    # The until timestamp in the fetch_trades call is exclusive, so we don't fetch trades at 'until' timestamp
                    # This means that all the trades we fetch in a new call are new, we can just write them all to the database
                    # without having to filter out previously fetched trades like we have to with binance

                        # write to database
                    l2_trades = super().normalize_to_l2(trades, 'Coinbase')
                    super().write_to_database(connection, l2_trades)

                    # only one trade happened in the one hour since start_time. We've already written that trade to database.
                    # increase start_time by an horu
                    # else:
                    #     end_time -= one_hour

                # no more new trades left to be fetched since 'since' timestamp
                else:
                    end_time = start_time - 1

                if (len(trades)):
                    print(len(trades))
                    print("-----")
                    print(trades[0])
                    print("-----")
                    print(trades[-1])

            except (ccxt.NetworkError, ccxt.BaseError) as e:
                print(type(e).__name__, str(e))
                time.sleep(RATE_LIMIT_SLEEP_TIME)
            count += 1
