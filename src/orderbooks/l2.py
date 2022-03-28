import json

from src.orderbooks.orderbook import Orderbook, LobUpdateError
from src.orderbooks.lob_enums import *


class L2Lob(Orderbook):
    def __init__(self):
        self.bids = {}
        self.asks = {}

    def handle_event(self, event: dict):
        self._check_fields(event)
        if event['lob_action'] == 2:
            self._insert(event)
        elif event['lob_action'] == 3:
            self._delete(event)
        elif event['lob_action'] == 4:
            self._update(event)
    
    def _insert(self, event):
        side, size, price = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        if price in book.keys():
            raise LobUpdateError("Inserting when price level already exists")
        book[price] = size

    def _delete(self, event):
        side, size, price = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
        if price not in book.keys():
            raise LobUpdateError("Deleting when price level doesn't exist")
        del book[price]

    def _update(self, event):
        side, size, price = self._unpack_event(event)
        book = self.bids if side == BID else self.asks
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