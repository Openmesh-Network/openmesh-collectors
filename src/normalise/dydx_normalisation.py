from helpers.util import create_lob_event, create_market_order
import json
import dateutil.parser
import time

class NormaliseDydx():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def normalise(self, data) -> dict:
        lob_events = []
        market_orders = []

        # Snapshot
        if data['type'] == 'subscribed' and data['channel'] == 'v3_orderbook':
            orders = data['contents']
            for bid in orders['bids']:
                self._handle_lob_event(bid, 1, data['receive_timestamp'], lob_events)
            for ask in orders['asks']:
                self._handle_lob_event(ask, 2, data['receive_timestamp'], lob_events)

        # Handling new LOB events
        elif data['type'] == 'channel_data' and data['channel'] == 'v3_orderbook':
            order_data = data['contents']
            for ask in order_data['asks']:
                self._handle_lob_event(ask, 2, data['receive_timestamp'], lob_events)
            for bid in order_data['bids']:
                self._handle_lob_event(bid, 1, data['receive_timestamp'], lob_events)

        elif data['type'] == 'channel_data' and data['channel'] == 'v3_trades':
            for trade in data['contents']['trades']:
                timestamp = time.mktime(dateutil.parser.isoparse(trade['createdAt']).timetuple())
                print(timestamp)
                market_orders.append(create_market_order(
                    order_id=self.ORDER_ID,
                    price=float(trade['price']),
                    timestamp=timestamp,
                    side=1 if trade['side'] == 'BUY' else 2,
                    size=float(trade['size'])
                ))
                self.ORDER_ID += 1

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

    def _handle_lob_event(self, data, side, receive_timestamp, lob_events):
        if isinstance(data, dict):
            price = float(data['price'])
            size = float(data['size'])
        else:
            price = float(data[0])
            size = float(data[1])
        if size == 0:
            lob_action = 3
            if price in self.ACTIVE_LEVELS:
                self.ACTIVE_LEVELS.remove(price)
        elif price in self.ACTIVE_LEVELS:
            lob_action = 4
        else:
            lob_action = 2
            self.ACTIVE_LEVELS.add(price)
        lob_events.append(create_lob_event(
            quote_no=self.QUOTE_NO,
            event_no=self.EVENT_NO,
            order_id=self.ORDER_ID,
            side=side,
            price=price,
            size=size if size else -1,
            lob_action=lob_action,
            receive_timestamp=receive_timestamp,
            order_type=0
        ))
        self.QUOTE_NO += 1
        self.ORDER_ID += 1
        self.EVENT_NO += 1
