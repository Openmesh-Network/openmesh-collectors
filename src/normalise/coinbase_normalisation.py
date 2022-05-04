from helpers.util import create_lob_event, create_market_order
from queue import Queue
import json


class NormaliseCoinbase():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_ORDER_IDS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    LONG_MAX = 2**63-1

    def __init__(self):
        self.snapshot_received = None
        self.lob_event_queue = Queue()

    def normalise(self, data) -> dict:
        lob_events = []
        market_orders = []

        if 'asks' in data:
            self.snapshot_received = float(data['sequence'])
            for ask in data['asks']:
                order_id = ask[2]
                price = float(ask[0])
                size = float(ask[1])
                side = 2
                event = create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=order_id,
                    side=side,
                    price=price,
                    size=size,
                    lob_action=2
                )
                lob_events.append(event)
                self.QUOTE_NO += 1
            for bid in data['bids']:
                order_id = bid[2]
                price = float(bid[0])
                size = float(bid[1])
                side = 1
                event = create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=order_id,
                    side=side,
                    price=price,
                    size=size,
                    lob_action=2
                )
                lob_events.append(event)
                self.QUOTE_NO += 1
            while not self.lob_event_queue.empty():
                event = self.lob_event_queue.get()
                if float(event['sequence']) >= self.snapshot_received:
                    self._handle_lob_event(event)

        # If the message is not a trade or a book update, ignore it. This can be seen by if the 'type' of the response is 'subscriptions'.
        elif data['type'] == 'subscriptions':
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS
        
        elif data['type'] == 'match':
            size = float(data['size'])
            price = float(data['price'])
            side = 1 if data['side'] == 'buy' else 2
            order_id = data['maker_order_id']

            #print(data)

            market_orders.append(create_market_order(
                    order_id = order_id,
                    trade_id = int(data['trade_id']),
                    price = price,
                    timestamp = data['time'],
                    side = side,
                    size = size,
                    msg_original_type = data["type"]
                ))
            self.ORDER_ID += 1

        elif data['type'] in ['done', 'open', 'change']:
            event = self._handle_lob_event(data)
            if event:
                lob_events.append(event)

        elif data['type'] == 'received':
            return self.NO_EVENTS

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

    def _handle_lob_event(self, data):
        # TODO: Implement order book snapshot at some point
        # if not self.snapshot_received:
        #    self.lob_event_queue.put(data)
        #    return

        if 'type' not in data.keys():
            return 

        if data['type'] == 'open':
            order_id = data['order_id']
            side = data['side']
            price = float(data['price'])
            size = float(data['remaining_size'])

            self.ACTIVE_ORDER_IDS.add(order_id)
            event = create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=1 if side == 'buy' else 2,
                price=price,
                size=size,
                lob_action=2,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            )
            self.QUOTE_NO += 1

        elif data['type'] == 'done':
            order_id = data['order_id']
            if order_id in self.ACTIVE_ORDER_IDS:
                self.ACTIVE_ORDER_IDS.remove(order_id)
            side = 1 if data['side'] == 'buy' else 2
            #size = float(data['size'])
            event = create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=1 if side == 'buy' else 2,
                price=-1,
                size=-1,
                lob_action=3,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            )          
            self.QUOTE_NO += 1

        elif data['type'] == 'change':
            new_size = float(data['new_size'])
            price = float(data['price'])
            side = 1 if data['side'] == 'buy' else 2
            order_id = data['order_id']

            event = create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=order_id,
                side=side,
                price=price,
                size=new_size,
                lob_action=4,
                event_timestamp=data['time'],
                receive_timestamp=data['receive_timestamp']
            )
            self.QUOTE_NO += 1

        if event:
            return event