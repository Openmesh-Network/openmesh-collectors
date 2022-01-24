from table import TableUtil

import json

class NormaliseBitfinex():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_ORDER_IDS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    LONG_MAX = 2**63-1

    def __init__(self, book_id: int, trades_id: int):
        self.util = TableUtil()
        self.book_id = book_id
        self.trades_id = trades_id

    def normalise(self, data) -> dict:
        """Rayman"""
        lob_events = []
        market_orders = []

        recv_ts = data[-1]
        if data[0] == self.book_id:
            if isinstance(data[1][0], list): # Snapshot
                for order in data[1]:
                    self._handle_lob_event(data, lob_events, order, 2)
                    self.ACTIVE_ORDER_IDS.add(order[0])
            else:
                order = data[1]
                side = 1 if order[2] > 0 else 2
                if order[2] == 0:
                    lob_action = 3
                    self.ACTIVE_ORDER_IDS.remove(order[0])
                elif order[0] in self.ACTIVE_ORDER_IDS:
                    lob_action = 4 # Update
                else:
                    lob_action = 2 # Insert
                    self.ACTIVE_ORDER_IDS.add(order[0])
                self._handle_lob_event(data, lob_events, order, lob_action)
        elif data[0] == self.trades_id:
            if isinstance(data[1][0], list):
                for trade in data[1]:
                    return
        else:
            print(f"Received message {data}")
            return self.NO_EVENTS
        self.EVENT_NO += 1

        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised
    
    def _handle_lob_event(self, data, lob_events, order, lob_action):
        side = 1 if order[2] > 0 else 2
        lob_events.append(self.util.create_lob_event(
            quote_no = self.QUOTE_NO,
            event_no = self.EVENT_NO,
            order_id = order[0],
            side = side,
            price = float(order[1]),
            size = order[2],
            lob_action = lob_action,
            send_timestamp = int(data[-1]),
            event_timestamp = int(data[-1]),
            receive_timestamp = data[-1],
            order_type = 0
        ))
        self.QUOTE_NO += 1

    def _handle_trade(self, data, market_orders, order):
        side = 1 if order[2] > 0 else 2
        market_orders.append(self.util.create_market_order())