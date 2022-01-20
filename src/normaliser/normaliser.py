"""
normaliser

Mediator class which provides a wrapper for normalising data from a websocket feed.
Instantiates the correct websocket connection with an ID.
"""
from threading import Thread
from time import sleep

from .manager.ws_factories import FactoryRegistry
from .normalising_strategies import NormalisingStrategies
from .tables.table import LobTable, MarketOrdersTable
from .order_book import OrderBookManager


class Normaliser():
    def __init__(self, exchange_id):
        # Initialise WebSocket handler
        self.ws_manager = FactoryRegistry().get_ws_manager(exchange_id)

        # Retrieve correct normalisation function
        self.normalise = NormalisingStrategies().get_strategy(exchange_id)

        # Initialise tables
        self.lob_table = LobTable()
        self.market_orders_table = MarketOrdersTable()
        self.order_book_manager = OrderBookManager()

        # Start normalising the data
        self.normalise_thr = Thread(
            name="normalising_thread",
            target=self._normalise_thread,
            args=(),
            daemon=True
        )
        self.normalise_thr.start()

    def put_entry(self, data: dict):
        """
        Puts data into the table.

        If you've implemented your normalisation algorithm correctly, it should automatically put your 
        data into the correct table.
        """
        data = self.normalise(data)
        lob_events = data["lob_events"]
        market_orders = data["market_orders"]

        for event in lob_events:
            if len(event) == 22:
                self.lob_table.put_dict(event)
                if event['lob_action'] == 2:
                    self.order_book_manager.insert({"price" : event['price'], "size" : event['size'], "side" : event['side']})
                elif event['lob_action'] == 3:
                    self.order_book_manager.delete({"price" : event['price'], "size" : event['size'], "side" : event['side']})
                elif event['lob_action'] == 4:
                    self.order_book_manager.update({"price" : event['price'], "size" : event['size'], "side" : event['side']})

        for order in market_orders:
            if len(order) == 6:
                self.market_orders_table.put_dict(order)

    def get_lob_events(self):
        return self.lob_table

    def get_best_orders(self):
        return self.order_book_manager.best_buy_order, self.order_book_manager.best_sell_order

    def get_market_orders(self):
        return self.market_orders_table

    def dump(self):
        print("LOB Data Table")
        self.lob_table.dump()
        print("Market Order Data Table")
        self.market_orders_table.dump()
        print("Order Book")
        self.order_book_manager.dump()

    def _normalise_thread(self):
        while True:
            # NOTE: This function blocks when there are no messages in the queue.
            data = self.ws_manager.get_msg()
            self.put_entry(data)
