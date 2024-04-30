import ccxt
import datetime
import pytz
from historical_data_collectors.base_data_collector import BaseDataCollector

class CoinbaseDataCollector(BaseDataCollector):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        self.exchange = ccxt.coinbase()
        self.markets = self.exchange.load_markets()
        self.symbols = self.exchange.symbols

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""
        # super().fetch_and_write_trades(start_date, end_date)
        self.fetch_and_write_symbol_trades('BTC/USDT', start_date, end_date)

    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date):
        
        # print('here')
        # print(symbol)
        # print(self.markets[symbol])

        utc_timezone = pytz.utc

        # in milliseconds
        start_time = int(
            datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)
        end_time = int(
            datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)

        one_hour = 3600 * 1000

        count = 0
        previous_trade_id = None

        while start_time < end_time and count < 3:

            try:

                # Binance api returns the lesser of the next 500 trades since start_time or all the trades in the hour
                # since start_time
                trades = self.exchange.fetch_trades(symbol, since = start_time, until= end_time)
                print(self.exchange.iso8601(start_time), len(trades), 'trades')

                if len(trades):
                    last_trade = trades[-1]

                    if previous_trade_id != last_trade['id']:

                        start_time = last_trade['timestamp']
                        previous_trade_id = last_trade['id']

                        # If this is the first page of trades we've fetched, we need to write all the trades
                        if previous_trade_id == None:
                            trades_to_write = trades

                        # Else, the first trade was written to db with the last page of trades
                        else:
                            trades_to_write = trades[1:]

                        # for trade in trades_to_write:

                        # write to database
                        # csv_writer.writerow({
                        #     'timestamp': trade['timestamp'],
                        #     'size': trade['amount'],
                        #     'price': trade['price'],
                        #     'side': trade['side'],
                        # })

                    # only one trade happened in the one hour since start_time. We've already written that trade to database.
                    # increase start_time by an horu
                    else:
                        start_time += one_hour

                # no trades were made in the one hour since start_time. Increase it by an hour
                else:
                    start_time += one_hour

                print(len(trades))
                print("-----")
                print(trades[0])
                print("-----")
                print(trades[-1])

            except ccxt.NetworkError as e:
                print(type(e).__name__, str(e))
            count += 1