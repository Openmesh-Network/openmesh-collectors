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


class Normaliser():
    def __init__(self, exchange_id):
        # Initialise WebSocket handler
        self.ws_manager = FactoryRegistry().get_ws_manager(exchange_id)

        # Retrieve correct normalisation function
        self.normalise = NormalisingStrategies().get_strategy(exchange_id)

        # Initialise tables
        self.lob_table = LobTable()
        self.market_orders_table = MarketOrdersTable()

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

        for order in market_orders:
            if len(order) == 6:
                self.market_orders_table.put_dict(order)

    def get_lob_events(self):
        return self.lob_table

    def get_best_orders(self):
        lob = self.get_lob_events()
        best_bid_price = -1
        best_ask_price = 10e9
        best_bid, best_ask = None, None

        for order in lob.table:
            if order['lob_action'] != 2:
                continue
            if order["side"] == 1:
                if order["price"] > best_bid_price:
                    best_bid_price = order["price"]
                    best_bid = order
            elif order["side"] == 2:
                if order["price"] < best_ask_price:
                    best_ask_price = order["price"]
                    best_ask = order

        return best_bid, best_ask

    def get_market_orders(self):
        return self.market_orders_table

    def dump(self):
        print("LOB Data Table")
        self.lob_table.dump()
        print("Market Order Data Table")
        self.market_orders_table.dump()

    def _normalise_thread(self):
        while True:
            # NOTE: This function blocks when there are no messages in the queue.
            data = self.ws_manager.get_msg()
            self.put_entry(data)
