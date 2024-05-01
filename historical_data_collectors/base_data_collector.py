import ccxt
from abc import ABC, abstractmethod
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv, find_dotenv
import os
import sys
import time

DOTENV_PATH = '/home/pythia/openmesh-collectors/historical_data_collectors/config.env'

class BaseDataCollector(ABC):

    exchange = None
    
    @abstractmethod
    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        connection = self.connect_to_postgres()
        # count = 0

        for symbol in self.symbols:

            #assuming we only need spot data
            if self.markets[symbol]['type'] == 'spot':
                print(symbol)
                self.fetch_and_write_symbol_trades(symbol, start_date, end_date, connection)
                # break

            # count += 1

            # if count >= 20:
            #     break
    
    @abstractmethod
    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date, connection):
        """Fetch and write all trades of symbol from start_date to end_date into the database"""

    
    # def write_to_db(self, trades, connection):
    #     """Writes the trades data into the database"""

    #     # trade = trades[0]
    #     # print("trade")
    #     # print(trade)
    #     # # data = {'exchange': trade, 'timestamp': trade['timestamp']}
    #     self.write_to_database(connection, trades)


    def connect_to_postgres(self):

        load_dotenv(DOTENV_PATH)

        try:
            # Connect to your PostgreSQL database
            connection = psycopg2.connect(
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
                database=os.getenv("DB_NAME")
            )
            print("Connected to PostgreSQL successfully!")
            return connection
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return None

    def write_to_database(self, connection, data):

        call_start = time.time()
        try:
            cursor = connection.cursor()

            # SQL statement for batch insert
            sql = """
            INSERT INTO l2_trades_test (exchange, symbol, price, size, taker_side, trade_id, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            # Execute batch insert
            psycopg2.extras.execute_batch(cursor, sql, data)


            # cursor.execute("""
            #     INSERT INTO l2_trades_test (exchange, symbol, price, size, taker_side, trade_id, timestamp)
            #     VALUES (%s, %s, %s, %s, %s, %s, %s)
            # """, (data['exchange'], data['symbol'], data['price'], data['size'], data['taker_side'], data['trade_id'], data['timestamp']))
            
            connection.commit()

            print("Data inserted successfully!")
        except (Exception, psycopg2.Error) as error:
            print("Error while writing to PostgreSQL:", error)
        finally:
            # Close the cursor 
            if cursor:
                cursor.close()

        call_end = time.time()
        call_time = call_end - call_start
        print(f"The database write took {call_time} to execute")