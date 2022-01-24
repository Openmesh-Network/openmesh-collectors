from table import TableUtil

import json


class NormaliseCoinbase():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_ORDER_IDS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    LONG_MAX = 2**63-1

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the 'type' of the response is 'subscriptions'.
        if data['type'] == 'subscriptions':
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        elif data['type'] == 'open':
            order_id = data['order_id']
            side = data['side']
            price = float(data['price'])
            size = float(data['remaining_size'])

            self.ACTIVE_ORDER_IDS.add(order_id)
            lob_events.append(self.util.create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=1 if side == 'buy' else 2,
                price=price,
                size=size,
                lob_action=2,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            ))

        elif data['type'] == 'done' and 'price' in data:
            order_id = data['order_id']
            if order_id in self.ACTIVE_ORDER_IDS:
                self.ACTIVE_ORDER_IDS.remove(order_id)
            price = float(data['price'])
            side = 1 if data['side'] == 'buy' else 2
            #size = float(data['size'])
            lob_events.append(self.util.create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=1 if side == 'buy' else 2,
                price=price,
                size=-1,
                lob_action=3,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            ))

        elif data['type'] == 'change':
            new_size = float(data['new_size'])
            price = float(data['price'])
            side = 1 if data['side'] == 'buy' else 2
            order_id = data['order_id']

            lob_events.append(self.util.create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=side,
                price=price,
                size=new_size,
                lob_action=4,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            ))
        
        elif data['type'] == 'match':
            size = float(data['size'])
            price = float(data['price'])
            side = 1 if data['side'] == 'buy' else 2
            order_id = data['maker_order_id']

            market_orders.append(self.util.create_market_order(
                    order_id = order_id,
                    trade_id = int(data['trade_id']),
                    price = price,
                    timestamp = data['time'],
                    side = side,
                    size = size,
                    msg_original_type = data["type"]
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
                    event_timestamp = data['time'],
                    receive_timestamp = data["receive_timestamp"],
                    order_type = 2,
                    order_executed = 1,
                    execution_price = price,
                    executed_size = size,
                    aggressor_side = side,
                    matching_order_id = data['taker_order_id'],
                    old_order_id = data['taker_order_id'],
                    trade_id = int(data['trade_id'])
                ))

            self.QUOTE_NO += 1

        # If the data is in an unexpected format, ignore it
        else:
            print(f"Received unrecognised message {json.dumps(data)}")
            return self.NO_EVENTS
        self.EVENT_NO += 1

        # Creating final normalised data dictionary which will be returned to the Normaliser
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised
