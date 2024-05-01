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


# from datetime import datetime, date


# def get_product_ids():
#     api_url = 'https://api.exchange.coinbase.com/products'
#     response = requests.get(api_url)
#     response_json = response.json()
#     # print(response_json[0])

#     product_ids = []

#     for product in response_json:
#         product_ids.append(product['id'])

#     print('number of product ids', len(product_ids))
#     return product_ids

# def get_oldest_trade_date(trades):

#     datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'
#     oldest_trade = trades[-1]
#     oldest_trade_date = datetime.strptime(oldest_trade['time'], datetime_format)
#     print('oldest_trade date', oldest_trade_date.date())

#     return oldest_trade_date.date()

# def get_product_trades(product_id, from_date, to_date):
# # def get_product_trades(product_id):

#     #curr_date tracks the date to which we've fetched data until
#     curr_date = to_date
#     pag_param = None
#     params = {}

#     api_url = 'https://api.exchange.coinbase.com/products/'+ product_id +'/trades'

#     count = 0

#     while (curr_date >= from_date):
#         if pag_param:
#             params['after'] = pag_param
#             response = requests.get(api_url, params = params)
#         else:
#             response = requests.get(api_url)

#         pag_param = response.headers.get('cb-after')
#         # print('pagination param:', pag_param)

#         response_json = response.json()
#         curr_date = get_oldest_trade_date(response_json)
#         count += 1

#         print('fetched trades for', product_id)
#         print('oldest date fetched', curr_date)
#         print('newest trade', response_json[0])
#         print('oldest trade\'s trade ', response_json[-1])
#         print('total num of times fetched', count)
#         print('-------------------')

#     return response_json

# def insert_trades_to_db(product, trades):

#     try:
#         conn = psycopg2.connect(
#             host="localhost",
#             database="openmesh_trades",
#             user="postgres",
#             password="DG.pg098"
#         )

#         cur = conn.cursor()
#         for item in trades:
#             cur.execute("""
#                             INSERT INTO trades (exchange, trade_pair, price, size, tradeid, timestamp)
#                             VALUES ('coinbase', %s, %s, %s, %s, %s);
#                         """, (product, item['price'], item['size'], item['trade_id'], item['time']))

#         conn.commit()
#         print('rowcount', cur.rowcount)
#         # print('successfully inserted')
#     except psycopg2.Error as e:
#         print(f"Database error: {e}")


# def old_main():

    # if len(sys.argv) != 3:
    #     print("Usage: python3 historical_runner.py from_date to_date")
    #     sys.exit(1)
    #
    # arg_date_format = "%Y/%m/%d"
    # from_date = datetime.strptime(sys.argv[1], arg_date_format).date()
    # to_date = datetime.strptime(sys.argv[2], arg_date_format).date()
    #
    # product_ids = get_product_ids()
    # product_trades = get_product_trades(product_ids[0], from_date, to_date)
    # insert_trades_to_db(product_ids[0], product_trades[0:2])


# def handle_symbol_trades(exchange_object, symbol, start_date, end_date):

#     utc_timezone = pytz.utc

#     #in milliseconds
#     start_time = int(datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp()*1000)
#     end_time = int(datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp()*1000)

#     one_hour = 3600 * 1000

#     count = 0
#     previous_trade_id = None

#     while start_time < end_time and count < 3:

#         try:

#             #Binance api returns the lesser of the next 500 trades since start_time or all the trades in the hour
#             #since start_time
#             trades = exchange_object.fetch_trades(symbol, since= start_time)
#             print(exchange_object.iso8601(start_time), len(trades), 'trades')

#             if len(trades):
#                 last_trade = trades[-1]

#                 if previous_trade_id != last_trade['id']:

#                     start_time = last_trade['timestamp']
#                     previous_trade_id = last_trade['id']

#                     #If this is the first page of trades we've fetched, we need to write all the trades
#                     if previous_trade_id == None:
#                         trades_to_write = trades

#                     #Else, the first trade was written to db with the last page of trades
#                     else:
#                         trades_to_write = trades[1:]


#                     # for trade in trades_to_write:

#                         #write to database
#                         # csv_writer.writerow({
#                         #     'timestamp': trade['timestamp'],
#                         #     'size': trade['amount'],
#                         #     'price': trade['price'],
#                         #     'side': trade['side'],
#                         # })

#                 #only one trade happened in the one hour since start_time. We've already written that trade to database.
#                 #increase start_time by an horu
#                 else:
#                     start_time += one_hour

#             #no trades were made in the one hour since start_time. Increase it by an hour
#             else:
#                 start_time += one_hour

#             print(len(trades))
#             print("-----")
#             print(trades[0])
#             print("-----")
#             print(trades[-1])

#         except ccxt.NetworkError as e:
#             print(type(e).__name__, str(e))
#         count += 1

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
