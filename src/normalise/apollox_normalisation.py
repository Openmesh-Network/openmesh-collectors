import json

from helpers.util import create_lob_event, create_market_order

class NormaliseApolloX():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    LONG_MAX = 2**63-1

    def __init__(self):
        self.last_update_id = -1
        self.caught_up = False
        self.last_u = None

    def normalise(self, data) -> dict:
        lob_events = []
        market_orders = []

        if "lastUpdateId" in data.keys():
            self.last_update_id = data["lastUpdateId"]
            for bid in data["bids"]:
                self._handle_lob_event(data, lob_events, bid, 1, 2, snapshot=True) # Insert
                self.ACTIVE_LEVELS.add(bid[0])
            for ask in data["asks"]:
                self._handle_lob_event(data, lob_events, ask, 2, 2, snapshot=True) # Insert
                self.ACTIVE_LEVELS.add(ask[0])
        elif "e" in data.keys() and data["e"] == "depthUpdate":
            u = data["u"]
            U = data["U"]
            if u <= self.last_update_id:
                return self.NO_EVENTS
            elif not self.caught_up and (U <= self.last_update_id + 1 and u >= self.last_update_id + 1):
                for bid in data["b"]:
                    lob_action = self._get_lob_action(bid)
                    if lob_action == 1:
                        continue
                    self._handle_lob_event(data, lob_events, bid, 1, lob_action)
                for ask in data["a"]:
                    lob_action = self._get_lob_action(ask)
                    if lob_action == 1:
                        continue
                    self._handle_lob_event(data, lob_events, ask, 2, lob_action)
                self.caught_up = True
                self.last_u = u
            elif self.last_u and U == self.last_u + 1:
                for bid in data["b"]:
                    lob_action = self._get_lob_action(bid)
                    if lob_action == 1:
                        continue
                    self._handle_lob_event(data, lob_events, bid, 1, lob_action)
                for ask in data["a"]:
                    lob_action = self._get_lob_action(ask)
                    if lob_action == 1:
                        continue
                    self._handle_lob_event(data, lob_events, ask, 2, lob_action)
                self.last_u = u

        elif "e" in data.keys() and data["e"] == "aggTrade":
            self._handle_trade(data, market_orders)
        else:
            print(f"Received unrecognised message {json.dumps(data)}")
            return self.NO_EVENTS
        self.EVENT_NO += 1
    
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised
    
    def _handle_lob_event(self, data, lob_events, order, side, lob_action, snapshot=False):
        lob_events.append(create_lob_event(
            quote_no = self.QUOTE_NO,
            event_no = self.EVENT_NO,
            side = side,
            price = float(order[0]),
            size = float(order[1]),
            lob_action = lob_action,
            send_timestamp = data["receive_timestamp"] if snapshot else data["E"],
            event_timestamp = data["receive_timestamp"] if snapshot else data["E"],
            receive_timestamp = data["receive_timestamp"],
            order_type = 0
        ))
        self.QUOTE_NO += 1

    def _handle_trade(self, data, market_orders):
        market_orders.append(create_market_order(
            order_id = self.ORDER_ID,
            price = float(data["p"]),
            trade_id = data["a"],
            timestamp = data["E"],
            side = 1 if data["m"] else 2,
            size = float(data["q"]),
            msg_original_type = data["e"]
        ))
        self.ORDER_ID += 1
    
    def _get_lob_action(self, order):
        if float(order[1]) == 0:
            if order[0] in self.ACTIVE_LEVELS:
                self.ACTIVE_LEVELS.remove(order[0])
                return 3 # Remove
            return 1
        elif order[0] in self.ACTIVE_LEVELS:
            return 4
        else:
            self.ACTIVE_LEVELS.add(order[0])
            return 2