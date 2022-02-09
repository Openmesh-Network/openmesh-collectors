from table import TableUtil
from queue import Queue
import json

class NormaliseHuobi():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()
        self.lob_event_queue = Queue()
        self.snapshot_received = None

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "type" key.
        if 'subbed' in data:
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Snapshot
        if 'data' in data:
            self.snapshot_received = int(data['data']['seqNum'])
            for ask in data['data']['asks']:
                price = float(ask[0])
                size = float(ask[1])
                side = 2
                event = self.util.create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=side,
                    price=price,
                    size=size,
                    lob_action=2
                )
                lob_events.append(event)
                self.EVENT_NO += 1
            for bid in data['data']['bids']:
                price = float(bid[0])
                size = float(bid[1])
                side = 1
                event = self.util.create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=side,
                    price=price,
                    size=size,
                    lob_action=2
                )
                lob_events.append(event)
            while not self.lob_event_queue.empty():
                event = self.lob_event_queue.get()
                if float(event['tick']['seqNum']) >= self.snapshot_received:
                    self._handle_lob_events(event, lob_events)

        # Handling new LOB events
        #print(json.dumps(data, indent=4))
        elif 'tick' in data and 'asks' in data['tick']:
            if self.snapshot_received is None:
                self.lob_event_queue.put(data)
            else:
                self._handle_lob_events(data, lob_events)
                

        elif 'tick' in data and "data" in data['tick']:
            trades = data['tick']['data']
            for trade in trades:
                market_orders.append(self.util.create_market_order(
                    order_id=self.ORDER_ID,
                    trade_id=trade['tradeId'],
                    price=float(trade['price']),
                    timestamp=float(trade['ts']),
                    side=1 if trade['direction'] == 'buy' else 2,
                    size=float(trade['amount']),
                ))
                self.ORDER_ID += 1

        # If the data is in an unexpected format, ignore it
        else:
            print(f"Received unrecognised message {json.dumps(data)}")
            return self.NO_EVENTS

        # Creating final normalised data dictionary which will be returned to the Normaliser
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised

    def _handle_lob_events(self, data, lob_events):
        ts = float(data['ts'])
        order_data = data['tick']
        for ask in order_data['asks']:
            price = float(ask[0])
            size = float(ask[1])
            # For Huobi, if the order size is 0, it means that the level is being removed
            if size == 0:
                lob_action = 3
                if price in self.ACTIVE_LEVELS:
                    self.ACTIVE_LEVELS.remove(price)
            # If the price is already in the active levels, it means that the level is being updated with a new size
            elif price in self.ACTIVE_LEVELS:
                lob_action = 4
            # Otherwise, it means that a new price level is being inserted
            else:
                lob_action = 2
                self.ACTIVE_LEVELS.add(price)
            # Once the nature of the lob event has been determined, it can be created and added to the list of lob events
            lob_events.append(self.util.create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=self.ORDER_ID,
                side=2,
                price=price,
                size=size if size else -1,
                lob_action=lob_action,
                send_timestamp=ts,
                receive_timestamp=data["receive_timestamp"],
                order_type=0
            ))
            self.QUOTE_NO += 1
            self.ORDER_ID += 1
            self.EVENT_NO += 1
        for bid in order_data['bids']:
            price = float(bid[0])
            size = float(bid[1])
            if size == 0:
                lob_action = 3
                if price in self.ACTIVE_LEVELS:
                    self.ACTIVE_LEVELS.remove(price)
                self.QUOTE_NO += 1
            elif price in self.ACTIVE_LEVELS:
                lob_action = 4
            else:
                lob_action = 2
                self.ACTIVE_LEVELS.add(price)
            lob_events.append(self.util.create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                order_id=self.ORDER_ID,
                side=1,
                price=price,
                size=size if size else -1,
                lob_action=lob_action,
                send_timestamp=ts,
                receive_timestamp=data["receive_timestamp"],
                order_type=0
            ))
            self.QUOTE_NO += 1
            self.ORDER_ID += 1
            self.EVENT_NO += 1

    