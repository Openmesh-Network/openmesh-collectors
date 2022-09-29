from helpers.util import create_lob_event, create_market_order
import json

class NormaliseKraken():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_BID_LEVELS = set()
    ACTIVE_ASK_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def normalise(self, data) -> dict:
        # This function currently only supports LOB events and trade data.
        lob_events = []
        market_orders = []

        # Kraken specific feed data parsing
        if isinstance(data, dict):
            if not ("event" in data.keys() and data["event"] == "heartbeat"):
                print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        recv_ts = data[-1]
        feed = data[-3] # data[-3] is the channel name
        if feed == "book-1000": 
            for i in range(1, min(3, len(data)-2)):
                events = data[i] # Dictionary of orderbook snapshot/updates
                if isinstance(events, dict):
                    if "bs" in events.keys(): # Snapshot bids
                        for bid in events["bs"]:
                            self._handle_lob_update("bs", lob_events, bid, 1, recv_ts)
                    if "as" in events.keys(): # Snapshots asks
                        for ask in events["as"]:
                            self._handle_lob_update("as", lob_events, ask, 2, recv_ts)
                    if "a" in events.keys(): 
                        for ask in events["a"]:
                            self._handle_lob_update("a", lob_events, ask, 2, recv_ts)
                    if "b" in events.keys(): 
                        for bid in events["b"]:
                            self._handle_lob_update("b", lob_events, bid, 1, recv_ts)
        elif feed == "trade":
            for trade in data[1]:
                self._handle_market_order(market_orders, trade)
        else:
            print(f"Received message {json.dumps(data)}")
            print(feed)
            return self.NO_EVENTS
        self.EVENT_NO += 1

        # Creating final normalised data dictionary which will be returned to the Normaliser
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }
        return normalised
    
    def _handle_lob_update(self, key, lob_events, event, side, recv_ts):
        if len(event) == 4:
            return
        price = float(event[0])
        size = float(event[1])
        ts = int(float(event[2])*10**3)
        if key == "as" or key == "bs":
            lob_action = 2
            if key == "as":
                self.ACTIVE_ASK_LEVELS.add(price)
            else:
                self.ACTIVE_BID_LEVELS.add(price)
        elif key == "a":
            if size == 0.0:
                lob_action = 3
                if price in self.ACTIVE_ASK_LEVELS:
                    self.ACTIVE_ASK_LEVELS.remove(price)
            elif price not in self.ACTIVE_ASK_LEVELS:
                lob_action = 2
                self.ACTIVE_ASK_LEVELS.add(price)
            else:
                lob_action = 4
        elif key == "b":
            if size == 0.0:
                lob_action = 3
                if price in self.ACTIVE_BID_LEVELS:
                    self.ACTIVE_BID_LEVELS.remove(price)
            elif price not in self.ACTIVE_BID_LEVELS:
                lob_action = 2
                self.ACTIVE_BID_LEVELS.add(price)
            else:
                lob_action = 4

        lob_events.append(create_lob_event(
            quote_no=self.QUOTE_NO,
            event_no=self.EVENT_NO,
            side=side,
            price=price,
            size=size,
            lob_action=lob_action,
            send_timestamp=ts,
            receive_timestamp=recv_ts,
            order_type=0,
        ))
        self.QUOTE_NO += 1

    def _handle_market_order(self, market_orders, trade):
        market_orders.append(create_market_order(
            order_id=self.ORDER_ID,
            price=float(trade[0]),
            timestamp=int(float(trade[2])*10e3),
            side=1 if trade[3] == "b" else 2,
            size=float(trade[2]),
            msg_original_type=trade[4]
        ))
        self.ORDER_ID += 1