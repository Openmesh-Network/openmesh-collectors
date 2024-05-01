import ccxt
import datetime
import pytz
from historical_data_collectors.base_data_collector import BaseDataCollector
import time

ONE_HOUR_IN_SECONDS = 3600
ONE_HOUR_IN_MILLISECONDS = ONE_HOUR_IN_SECONDS * 1000
MAX_BINANCE_API_LIMIT = 1000

class BinanceDataCollector(BaseDataCollector):


    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        self.exchange = ccxt.binance()
        self.exchange.rateLimit = 250
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""
        super().fetch_and_write_trades(start_date, end_date)
        # connection = super().connect_to_postgres()
        # self.fetch_and_write_symbol_trades('ETH/USDT', start_date, end_date, connection)

    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date, connection):

        utc_timezone = pytz.utc

        # in milliseconds
        # start_time = int(
        #     datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)
        # end_time = int(
        #     datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)

        current_time = datetime.datetime.now()
        end_time = int(current_time.timestamp()*1000)


        one_hour_before = current_time - datetime.timedelta(hours=1)
        five_min_before = current_time - datetime.timedelta(minutes=5)
        one_minute_before = current_time - datetime.timedelta(minutes=1)
        one_second_before = current_time - datetime.timedelta(seconds=1)

        # start_time = int(one_second_before.timestamp() * 1000)
        # start_time = int(one_minute_before.timestamp() * 1000)
        # start_time = int(five_min_before.timestamp() * 1000)
        start_time = int(one_hour_before.timestamp() * 1000)

        # print("start time", one_minute_before)
        # print("end time", current_time)

        # count = 2
        previous_trade_id = None

        # while start_time < end_time and count < 1:
        while start_time < end_time:

            try:

                # Binance api returns the lesser of the next 500 trades since start_time or all the trades in the hour
                # since start_time

                call_start = time.time()
                trades = self.exchange.fetch_trades(symbol, since=start_time, limit = MAX_BINANCE_API_LIMIT)
                call_end = time.time()
                call_time = call_end - call_start
                print(f"The fetch call took {call_time} to execute")

                #Tried different ways to paginate but didn't work
                # if cursor:
                #     trades = self.exchange.fetch_trades(symbol, params = {'cursor': 0})
                # else:
                #     trades = self.exchange.fetch_trades(symbol, params = {'pagination': True})

                # trades = self.exchange.fetch_trades(symbol, since=start_date,  params = {'paginate': True, "paginationDirection": "forward"})
                # print(self.exchange.iso8601(start_time), len(trades), 'trades')

                print("--FETCHED---")
                print(len(trades), "trades")

                if len(trades):
                    print(trades[0])
                    print("-----")
                    print(trades[-1])

                if len(trades):

                    # print(self.exchange.last_response_headers.keys())

                    # cursor = self.exchange.last_response_headers['CB-AFTER']
                    last_trade = trades[-1]

                    if previous_trade_id != last_trade['id']:
                        
                        start = time.time()
                        #filter out any trades we've already fetched/written to the db in a past api call
                        print("previous_trade_id", previous_trade_id)
                        trades_to_write = self.filter_new_trades(trades, previous_trade_id)

                        # print("--TO WRITE---")
                        # print(len(trades))
                        # print(trades_to_write[0])
                        # print("-----")
                        # print(trades_to_write[-1])

                        start_time = last_trade['timestamp']
                        previous_trade_id = last_trade['id']

                        l2_trades = self.normalize_to_l2(trades_to_write)
                        end = time.time()
                        print(f"filtering and normalising the trades took {end-start} time")
                        self.write_to_database(connection, l2_trades)

                    # only one trade happened in the one hour since start_time. We've already written that trade to database.
                    # increase start_time by an horu
                    else:
                        start_time += ONE_HOUR_IN_MILLISECONDS

                # no trades were made in the one hour since start_time. Increase it by an hour
                else:
                    start_time += ONE_HOUR_IN_MILLISECONDS


                # print("--FETCHED---")
                # print(len(trades))
                # print(trades[0])
                # print("-----")
                # print(trades[-1])

            except ccxt.NetworkError as e:
                print(type(e).__name__, str(e))
            # count += 1

    def filter_new_trades(self, trades, previous_trade_id):
        """Returns trades that have occured after the previous_trade_id trade. 
        Effectively this returns trades that we've fetched for the first time from binance, 
        removing trades we've fetched previously"""

        #if we haven't fetched any trades before, all trades are new
        if previous_trade_id == None:
            return trades

        #if we have fetched trades before, check all the trades we've fetched now to find new ones
        for i in range(len(trades)):
            # print(trades[i]['id'], previous_trade_id)
            # print(trades[i]['id'] == previous_trade_id)
            #We've fetched trades occuring before this previously
            if trades[i]['id'] == previous_trade_id:
                # print(len(trades))
                # print(len())
                return trades[i+1:]

        #All trades are new
        return trades

    # def write_to_db(self, trades):
    #     super().write_to_db(trades)

    def normalize_to_l2(self, trades):

        normalised_data = []
        # trade = trades[0]

        for trade in trades:
            # trade_data = {'exchange': 'Binance', 'symbol': trade['symbol'], 'price': trade['price'], 'size': trade['amount'], 'taker_side': trade['side'], 'trade_id': trade['id'], 'timestamp': trade['timestamp']}
            trade_data = ('Binance', trade['symbol'], trade['price'], trade['amount'], trade['side'], trade['id'], trade['timestamp'])
            normalised_data.append(trade_data)

        return normalised_data