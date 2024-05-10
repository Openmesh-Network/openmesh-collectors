from abc import ABC, abstractmethod
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv, find_dotenv
import os
from ..helpers.profiler import Profiler
import datetime
import pytz
import sys

ENV_FILE = 'config.env'
ONE_SECOND_IN_MILLISECONDS = 1000

class BaseDataCollector(ABC):

    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""
        self.profiler = Profiler()
        self.connection = None
        

    def fetch_and_write_trades(self, start_date, end_date=None):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        # count = 0

        utc_timezone = pytz.utc

        #start time is the first second of the start date
        start_time = int(
            datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * ONE_SECOND_IN_MILLISECONDS)

        #Set end time to the first second of the end date if supplied
        if end_date is not None:
            end_time = int(
                datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * ONE_SECOND_IN_MILLISECONDS)
        
        #Else fetch records between start_date and now
        else:
            current_time = datetime.datetime.now()
            end_time = int(current_time.timestamp()*ONE_SECOND_IN_MILLISECONDS)

        #Iterate through all the symbols and fetch and write trades for the spot symbols
        for symbol in self.symbols:

            #assuming we only need spot data
            if self.markets[symbol]['type'] == 'spot':
                # print(symbol)
                self.fetch_and_write_symbol_trades(symbol, start_time, end_time)
                # break

            # count += 1

            # if count >= 30:
            #     break
    

    @abstractmethod
    # def fetch_and_write_symbol_trades(self, *args, **kwargs):
    def fetch_and_write_symbol_trades(self, symbol, start_time, end_time):
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

        #load env variables from ENV_FILE
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
        """Writes the data to the table defined in the TABLE_NAME env variable in the database connected to by the connection object"""

        load_dotenv(os.path.join(os.path.dirname(__file__), ENV_FILE))

        if self.connection is None or self.connection.closed:
            self.connection = self.connect_to_postgres()

        self.profiler.start('database write')

        try:
            cursor = self.connection.cursor()

            # SQL statement for batch insert
            #We use insert ignore as there might be an attempt to insert duplicate rows into the db especially if the 
            #data collectors are run parallelly on many machines
            sql = f"""
            INSERT INTO {os.getenv("TABLE_NAME")} (exchange, symbol, price, size, taker_side, trade_id, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (exchange, symbol, trade_id) DO NOTHING;
            """

                # Existing row: (%s, %s, %s, %s, %s, %s, %s). New row: (%s, %s, %s, %s, %s, %s, %s)', 
                #     exchange, symbol, trade_id,
                #     exchange, symbol, price, size, taker_side, trade_id, timestamp,
                #     EXCLUDED.exchange, EXCLUDED.symbol, EXCLUDED.price, EXCLUDED.size, EXCLUDED.taker_side, EXCLUDED.trade_id, EXCLUDED.timestamp
            # Execute batch insert
            psycopg2.extras.execute_batch(cursor, sql, data)

            self.connection.commit()

            print("Data inserted successfully!")
        
        except (Exception, psycopg2.Error) as error:
            print("Error while writing to PostgreSQL:", error)
            # print("data that was to be written \n", data)
            sys.exit(1)
        
        finally:
            # Close the cursor 
            if cursor:
                cursor.close()

        self.profiler.stop('database write')