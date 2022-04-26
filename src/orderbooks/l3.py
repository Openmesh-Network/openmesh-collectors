import json

from src.orderbooks.orderbook import Orderbook, LobUpdateError
from src.orderbooks.lob_enums import *


class L3Lob(Orderbook):
    def __init__(self):
        self.bids = {}
        self.asks = {}

        # Assume that all order_ids are unique
        self.active_orders = {}
        self.order_prices = {}
        self.order_history = {}

    def handle_event(self, event: dict):
        self._check_fields(event)
        side, size, price, order_id = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        try:
            if event['lob_action'] == INSERT:
                self._insert(book, size, price, order_id)
            elif event['lob_action'] == REMOVE:
                self._remove(book, order_id)
            elif event['lob_action'] == UPDATE:
                self._update(book, size, price, order_id)
            self._log_order(order_id, event['lob_action'], side, size, price)
        except (KeyError, ValueError):
            print(f"Order History for order {event['order_id']}")
            print(json.dumps(self.order_history[event['order_id']], indent=2))
            print(f"Event handling raised KeyError for {event}")

    def _insert(self, book, size, price, order_id):
        if order_id in self.active_orders.keys():
            raise LobUpdateError("Inserting when order id already exists")
        if price not in book.keys():
            book[price] = []
        order = {'size': size, 'order_id': order_id}
        book[price].append(order)
        self.active_orders[order_id] = order
        self.order_prices[order_id] = price

    def _remove(self, book, order_id):
        if order_id not in self.active_orders.keys():
            raise LobUpdateError("Removing when order id doesn't exist")
        price = self.order_prices[order_id]
        try:
            book[price].remove(self.active_orders[order_id])
        except (KeyError, ValueError):
            return
        del self.active_orders[order_id]
        del self.order_prices[order_id]
        if len(book[price]) == 0:
            del book[price]

    def _update(self, book, size, price, order_id):
        if order_id not in self.active_orders.keys():
            raise LobUpdateError("Updating when order id doesn't exist")
        order = self.active_orders[order_id]
        old_price = self.order_prices[order_id]
        if size > order['size'] or old_price != price:
            book[old_price].remove(self.active_orders[order_id])
            if price not in book.keys():
                book[price] = []
            book[price].append(self.active_orders[order_id])
        self._update_hashmaps(order_id, size, price)
    
    def _update_hashmaps(self, order_id, size, price):
        self.order_prices[order_id] = price
        self.active_orders[order_id]['size'] = size

    def _log_order(self, order_id, lob_action, side, size, price):
        if order_id not in self.order_history.keys():
            self.order_history[order_id] = []
        if lob_action == UPDATE:
            action = "update"
        elif lob_action == REMOVE:
            action = "remove"
        elif lob_action == INSERT:
            action = "insert"
        else:
            action = f"unknown_{lob_action}"
        self.order_history[order_id].append([action, side, price, size])

    def snapshot(self):
        lob = {'bids': self.bids, 'asks': self.asks}
        return json.dumps(lob)
    
    def get_ahead(self, event):
        """Will implement with Python's Decimal module for accurate 
        floating point summations.
        """
        return -1, -1
    
    def _check_fields(self, event):
        keys = event.keys()
        if 'price' not in keys or \
                'lob_action' not in keys or \
                'side' not in keys or \
                'order_id' not in keys or \
                'size' not in keys:
            raise KeyError("Key is not present in LOB event.")

    def _unpack_event(self, event: dict):
        return event['side'], event['size'], event['price'], event['order_id']