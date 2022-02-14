from table import TableUtil

import json

class NormaliseBitfinex():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_ORDER_IDS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    LONG_MAX = 2**63-1

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Rayman"""
        lob_events = []
        market_orders = []
    
        if not isinstance(data, list):
            print(f"Received message {data}")
            return self.NO_EVENTS
        
        if data[1] == "hb":
            print(f"Received message {data}")
            return self.NO_EVENTS

        if isinstance(data[1], list) and len(data[1]) == 3 or isinstance(data[1][0], list) and len(data[1][0]) == 3:
            if isinstance(data[1][0], list): # Snapshot
                for order in data[1]:
                    #print("Processing Order: ", order)
                    self._handle_lob_event(data, lob_events, order, 2)
                    self.ACTIVE_ORDER_IDS.add(order[0])
            else:
                order = data[1]
                #print("Processing Order: ", order)
                side = 1 if order[2] > 0 else 2
                if order[1] == 0:
                    lob_action = 3
                    if order[0] in self.ACTIVE_ORDER_IDS:
                        self.ACTIVE_ORDER_IDS.remove(order[0])
                    else:
                        return self.NO_EVENTS
                elif order[0] in self.ACTIVE_ORDER_IDS:
                    lob_action = 4 # Update
                else:
                    lob_action = 2 # Insert
                    self.ACTIVE_ORDER_IDS.add(order[0])
                self._handle_lob_event(data, lob_events, order, lob_action)
        #elif isinstance(data[1][-1], int):
        elif data[1] == "tu" or isinstance(data[1], list):
            if isinstance(data[1][0], list):
                for trade in data[1]:
                    self._handle_trade(data, market_orders, trade)
            elif data[1] == "tu":
                trade = data[2]
                self._handle_trade(data, market_orders, trade)
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
            size = abs(order[2]),
            lob_action = lob_action,
            send_timestamp = data[-1],
            event_timestamp = data[-1],
            receive_timestamp = data[-1],
            order_type = 0
        ))
        self.QUOTE_NO += 1

    def _handle_trade(self, data, market_orders, trade):
        side = 1 if trade[2] > 0 else 2
        print(trade)
        market_orders.append(self.util.create_market_order(
            order_id = self.ORDER_ID,
            price = trade[3],
            trade_id = trade[0],
            timestamp = trade[1],
            side = side,
            size = abs(trade[2]),
            msg_original_type = data[0]
        ))
        self.ORDER_ID += 1
