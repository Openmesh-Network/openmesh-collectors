import ccxt
from abc import ABC, abstractmethod
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv, find_dotenv
import os
import sys
import time
from .helpers.profiler import Profiler

ENV_FILE = 'config.env'

class BaseDataCollector(ABC):

    exchange = None
    

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        self.profiler = Profiler()
        self.connection = None

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        # self.connection = self.connect_to_postgres()
        # count = 0

        for symbol in self.symbols:

            #assuming we only need spot data
            if self.markets[symbol]['type'] == 'spot':
                print(symbol)
                self.fetch_and_write_symbol_trades(symbol, start_date, end_date)
                # break

            # count += 1

            # if count >= 20:
            #     break
    

    @abstractmethod
    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date):
        """Fetch and write all trades of symbol from start_date to end_date into the database"""


    def normalize_to_l2(self, trades, exchange_name):
        """Takes as arguments the fetched trades and the exchange name and returns the relevant data for the l2 trades schema
        as a tuple"""

        normalised_data = []

        for trade in trades:
            trade_data = (exchange_name, trade['symbol'], trade['price'], trade['amount'], trade['side'], trade['id'], trade['timestamp'])
            normalised_data.append(trade_data)

        return normalised_data


    def connect_to_postgres(self):
        """Establishes a connection with the database and returns a connection object"""

        load_dotenv(os.path.join(os.path.dirname(__file__), ENV_FILE))

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

    def write_to_database(self, data):
        """Writes the data to the database connected to by the connection object"""

        if self.connection is None or self.connection.closed:
            self.connection = self.connect_to_postgres()

        # call_start = time.time()
        self.profiler.start('database write')

        try:
            cursor = self.connection.cursor()

            # SQL statement for batch insert
            sql = """
            INSERT INTO l2_trades_test (exchange, symbol, price, size, taker_side, trade_id, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            # Execute batch insert
            psycopg2.extras.execute_batch(cursor, sql, data)

            self.connection.commit()

            print("Data inserted successfully!")
        except (Exception, psycopg2.Error) as error:
            print("Error while writing to PostgreSQL:", error)
        finally:
            # Close the cursor 
            if cursor:
                cursor.close()

        self.profiler.stop('database write')