from table import TableUtil

import json

class NormaliseBitfinex():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
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

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "type" key.
        if "topic" not in data.keys():
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS
        
        if "type" not in data.keys():
            if not "trade" in data["topic"]:
                raise ValueError(f"type not in keys and action is not a trade: {data['topic']}")
            for trade in data["data"]:
                side = 1 if trade["side"] == "Buy" else 2
                size = trade["size"]
                ts = int(trade["trade_time_ms"])
                price = float(trade["price"])
                trade_id = trade["trade_id"]
                order_id = int(price*10**4)
                market_orders.append(self.util.create_market_order(
                    order_id = order_id,
                    trade_id = trade_id,
                    price = price,
                    timestamp = ts,
                    side = side,
                    msg_original_type = trade["cross_seq"]
                ))
                self.ORDER_ID += 1
                lob_events.append(self.util.create_lob_event(
                    quote_no = self.QUOTE_NO,
                    event_no = self.EVENT_NO,
                    order_id = order_id,
                    side = side,
                    price = price,
                    size = size,
                    lob_action = 1, 
                    event_timestamp = ts,
                    receive_timestamp = data["receive_timestamp"],
                    order_type = 2,
                    order_executed = 1,
                    execution_price = price,
                    executed_size = size,
                    aggressor_side = side,
                    matching_order_id = int(price*10**4),
                    old_order_id = int(price*10**4),
                    trade_id = trade_id
                ))
                self.QUOTE_NO += 1
        else: 
            if data["type"] == "snapshot":
                for order in data["data"]:
                    lob_events.append(self.util.create_lob_event(
                        quote_no = self.QUOTE_NO,
                        event_no = self.EVENT_NO,
                        order_id = order["id"],
                        side = 1 if order["side"] == "Buy" else 2,
                        price = float(order["price"]),
                        size = order["size"],
                        lob_action = 2,
                        send_timestamp = int(data["timestamp_e6"]/10**3),
                        receive_timestamp = data["receive_timestamp"],
                        order_type = 0 
                    ))
                    self.QUOTE_NO += 1
            elif data["type"] == "delta":
                orders = data["data"]
                for order in orders["delete"]:
                    self._handle_lob_event(data, lob_events, order, 3)
                for order in orders["update"]:
                    self._handle_lob_event(data, lob_events, order, 4)
                for order in orders["insert"]:
                    self._handle_lob_event(data, lob_events, order, 2)
            else:
                print(f"Received unrecognised message {json.dumps(data)}")
                return self.NO_EVENTS
        self.EVENT_NO += 1

        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised
    
    def _handle_lob_event(self, data, lob_events, order, lob_action):
        if lob_action == 3:
            size = -1
        else:
            size = order["size"]
        lob_events.append(self.util.create_lob_event(
            quote_no = self.QUOTE_NO,
            event_no = self.EVENT_NO,
            order_id = order["id"],
            side = 1 if order["side"] == "Buy" else 2,
            price = float(order["price"]),
            size = size,
            lob_action = lob_action,
            send_timestamp = int(data["timestamp_e6"]/10**3),
            event_timestamp = int(data["data"]["transactTimeE6"]/10**3),
            receive_timestamp = data["receive_timestamp"],
            order_type = 0
        ))
        self.QUOTE_NO += 1