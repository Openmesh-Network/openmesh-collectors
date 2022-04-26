import json

from src.orderbooks.orderbook import Orderbook, LobUpdateError
from src.orderbooks.lob_enums import *


class L2Lob(Orderbook):
    def __init__(self):
        self.bids = {}
        self.asks = {}

    def handle_event(self, event: dict):
        self._check_fields(event)
        side, size, price = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        if event['lob_action'] == INSERT:
            self._insert(book, size, price)
        elif event['lob_action'] == REMOVE:
            self._remove(book, price)
        elif event['lob_action'] == UPDATE:
            self._update(book, size, price)
    
    def _insert(self, book, size, price):
        if price in book.keys():
            raise LobUpdateError("Inserting when price level already exists")
        book[price] = size

    def _remove(self, book, price):
        if price not in book.keys():
            raise LobUpdateError("Removing when price level doesn't exist")
        del book[price]

    def _update(self, book, size, price):
        if price not in book.keys():
            raise LobUpdateError("Updating when price level doesn't exist")
        book[price] = size
    
    def snapshot(self):
        lob = {'bids': self.bids, 'asks': self.asks}
        return json.dumps(lob)
    
    def _check_fields(self, event):
        keys = event.keys()
        if 'price' not in keys or \
                'lob_action' not in keys or \
                'side' not in keys or \
                'size' not in keys:
            raise KeyError("Key is not present in LOB event.")
    
    def _unpack_event(self, event: dict):
        return event['side'], event['size'], event['price']