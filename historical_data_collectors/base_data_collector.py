import ccxt
from abc import ABC, abstractmethod
import psycopg2

class BaseDataCollector(ABC):

    exchange = None
    
    @abstractmethod
    def __init__(self):
        """Initialises the ccxt exchange object, should be implemented by the subclasses"""

    def fetch_and_write_trades(self, start_date, end_date):
        """Fetches the L2 trades data from the relevant exchange API and writes that to the given database"""

        for symbol in self.symbols:

            #assuming we only need spot data
            if self.markets[symbol]['type'] == 'spot':
                print(symbol)
                self.fetch_and_write_symbol_trades(symbol, start_date, end_date)
                break
    
    @abstractmethod
    def fetch_and_write_symbol_trades(self, symbol, start_date, end_date):
        """Fetch and write all trades of symbol from start_date to end_date into the database"""

    
    def write_to_db(self, trades):
        """Writes the trades data into the database"""
        
        connection = self.connect_to_postgres()

        # trade = trades[0]
        # print("trade")
        # print(trade)
        # # data = {'exchange': trade, 'timestamp': trade['timestamp']}
        self.write_to_database(connection, trades)


    def connect_to_postgres(self):
        try:
            # Connect to your PostgreSQL database
            connection = psycopg2.connect(
                user="postgres",
                password="openmesh.collectors",
                host="localhost",
                port="5432",
                database="pythia"
            )
            print("Connected to PostgreSQL successfully!")
            return connection
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL:", error)
            return None

    def write_to_database(self, connection, data):
        try:
            cursor = connection.cursor()
            # Execute a SQL INSERT statement
            cursor.execute("""
                INSERT INTO l2_trades_test (exchange, symbol, price, size, taker_side, trade_id, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (data['exchange'], data['symbol'], data['price'], data['size'], data['taker_side'], data['trade_id'], data['timestamp']))
            connection.commit()
            print("Data inserted successfully!")
        except (Exception, psycopg2.Error) as error:
            print("Error while writing to PostgreSQL:", error)
        finally:
            # Close the cursor 
            if cursor:
                cursor.close()