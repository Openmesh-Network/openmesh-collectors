import json

from src.orderbooks.orderbook import Orderbook


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
        pass

    def _delete(self, event):
        pass

    def _update(self, event):
        pass
    
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