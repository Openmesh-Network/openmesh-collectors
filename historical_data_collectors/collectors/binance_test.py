import pytest
from .binance_data_collector import BinanceDataCollector
import datetime
from types import MethodType
import logging
import pytz


ONE_SECOND_IN_MILLISECONDS = 1000

def test_fetch_and_write_symbol_trades():
    """Tests the main function fetch_and_write_symbol_trades for the Binance Data Collector.
       It fetched the trades data for LPT token for the last hour of 2024/01/01.
       
       Due to the way the pagination in binance works the functions ends up fetching data until almost 1AM 2024/01/02.
       That's okay. We just test that it does correctly fetch the first and last trade id and all the trade ids in between"""
    
    data_collector = BinanceDataCollector()

    arg_date_format = "%Y/%m/%d"

    #exclusive
    end_date = datetime.datetime.strptime('2024/01/02', arg_date_format).date()


    utc_timezone = pytz.utc
    end_time = int(datetime.datetime.combine(end_date, datetime.datetime.min.time(),
                                              tzinfo=utc_timezone).timestamp() * ONE_SECOND_IN_MILLISECONDS)

    two_hour_before = datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS) - datetime.timedelta(hours=2)
    one_hour_before = datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS) - datetime.timedelta(hours=1)
    five_min_before = datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS)  - datetime.timedelta(minutes=5)
    one_minute_before = datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS) - datetime.timedelta(minutes=1)
    one_second_before = datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS) - datetime.timedelta(seconds=1)

    # start_time = int(one_second_before.timestamp() * 1000)
    # start_time = int(one_minute_before.timestamp() * 1000)
    # start_time = int(five_min_before.timestamp() * 1000)
    # start_time = int(two_hour_before.timestamp() * 1000)
    start_time = int(one_hour_before.timestamp() * 1000)

    print(datetime.datetime.fromtimestamp(start_time/ONE_SECOND_IN_MILLISECONDS))
    print(datetime.datetime.fromtimestamp(end_time/ONE_SECOND_IN_MILLISECONDS))

    data_collector.total_fetched_trades = []
    logging.info(len(data_collector.total_fetched_trades))

    def collect_trades(self, trades):

        self.total_fetched_trades += trades  

    #replace write to database with custom collect_trades function to record the trades
    data_collector.write_to_database = MethodType(collect_trades, data_collector)

    data_collector.fetch_and_write_symbol_trades('LPT/USDT', start_time, end_time)
    
    first_trade = data_collector.total_fetched_trades[0]
    last_trade = data_collector.total_fetched_trades[-1]

    assert first_trade[5] == '13679785'
    assert last_trade[5] == '13680923'

    #tests that all trades b/w the first and last trade id is fetched
    assert len(data_collector.total_fetched_trades) == int(last_trade[5]) - int(first_trade[5]) + 1