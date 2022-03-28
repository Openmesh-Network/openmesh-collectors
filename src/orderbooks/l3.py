import json

from src.orderbooks.orderbook import Orderbook, LobUpdateError
from src.orderbooks.lob_enums import *


class L3Lob(Orderbook):
    def __init__(self):
        self.bids = {}
        self.asks = {}

        # Assume that all order_ids are unique
        self.active_orders = {}

    def handle_event(self, event: dict):
        self._check_fields(event)
        side, size, price, order_id = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        if event['lob_action'] == INSERT:
            self._insert(book, size, price, order_id)
        elif event['lob_action'] == REMOVE:
            self._remove(book, price, order_id)
        elif event['lob_action'] == UPDATE:
            self._update(book, size, price, order_id)
    
    def _insert(self, book, size, price, order_id):
        if order_id in self.active_orders.keys():
            raise LobUpdateError("Inserting when order id already exists")
        if price not in book.keys():
            book[price] = []
        order = {'size': size, 'order_id': order_id}
        book[price].append(order)
        self.active_orders[order_id] = order

    def _remove(self, book, price, order_id):
        if order_id not in self.active_orders.keys():
            raise LobUpdateError("Removing when order id doesn't exists")
        book[price].remove(self.active_orders[order_id])
        del self.active_orders[order_id]
        if len(book[price]) == 0:
            del book[price]


    def _update(self, book, size, price, order_id):
        if order_id not in self.active_orders.keys():
            raise LobUpdateError("Updating when order id doesn't exists")
        order = self.active_orders[order_id]
        if size > order['size']:
            book[price].remove(self.active_orders[order_id])
            book[price].append(self.active_orders[order_id])
        order['size'] = size


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