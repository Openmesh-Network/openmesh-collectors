import requests
import sys
# import psycopg2
import ccxt
import datetime
import pytz
import time
# import paramiko
# from sshtunnel import SSHTunnelForwarder
# from paramiko.rsakey import RSAKey

from historical_data_collectors.binance_data_collector import BinanceDataCollector
from historical_data_collectors.coinbase_data_collector import CoinbaseDataCollector


def main():

    start_time = time.time()

    try:
        if len(sys.argv) != 4:
            print("Usage: python3 historical_runner.py exchange_name start_date end_date")
            sys.exit(1)

        exchange_name = sys.argv[1].lower()

        if exchange_name == 'binance':
            data_collector = BinanceDataCollector()
        elif exchange_name == 'coinbase':
            # print("before")
            data_collector = CoinbaseDataCollector()
            # print("after")
        elif exchange_name == 'dydx':
            data_collector = DydxDataCollector()
        elif exchange_name == 'bybit':
            data_collector = BybitDataCollector()
        elif exchange_name == 'okx':
            data_collector = OkxDataCollector()
        else:
            print(f"Exchange {exchange_name} is not supported. Currently supported exchanges are Binance, Coinbase, Dydx, Bybit and Okx")
            sys.exit(1)

        arg_date_format = "%Y/%m/%d"

        #inclusive
        start_date = datetime.datetime.strptime(sys.argv[2], arg_date_format).date()

        #exclusive
        end_date = datetime.datetime.strptime(sys.argv[3], arg_date_format).date()

        
        data_collector.fetch_and_write_trades(start_date, end_date)
    
    finally:
        end_time = time.time()

        # Calculate the total execution time
        execution_time = end_time - start_time

        print("Script execution time: {:.2f} seconds".format(execution_time))

    

if __name__ == "__main__":
    main()
