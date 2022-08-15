from helpers.util import create_lob_event, create_market_order
import json

class NormaliseKrakenFutures():
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
        if "event" in data.keys() or "feed" not in data.keys():
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Impossible to reconstruct L3 data without order ids, so only L2 granularity is achieved
        if data["feed"] == "book_snapshot":
            # There's no table column for the product_id/ticker??
            ts = data["timestamp"]
            for bid in data["bids"]:
                lob_events.append(create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    side=1,  # Buy order
                    price=bid["price"],
                    size=bid["qty"],
                    lob_action=2,
                    send_timestamp=ts,
                    # Manually appended timestamp (see websocket_manager.py)
                    receive_timestamp=data["receive_timestamp"],
                    order_type=0,
                ))
                self.ACTIVE_BID_LEVELS.add(bid["price"])
                self.QUOTE_NO += 1
            for ask in data["asks"]:
                lob_events.append(create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    side=2,  # Sell order
                    price=ask["price"],
                    size=ask["qty"],
                    lob_action=2,
                    send_timestamp=ts,
                    # Manually appended timestamp (see websocket_manager.py)
                    receive_timestamp=data["receive_timestamp"],
                    order_type=0,
                ))
                self.ACTIVE_ASK_LEVELS.add(ask["price"])
                self.QUOTE_NO += 1
        elif data["feed"] == "book":
            ts = data["timestamp"]
            price = data["price"]
            side = data["side"]
            qty = data["qty"]
            if qty == 0:
                lob_action = 3  # Remove level
                if side == "buy":
                    if price in self.ACTIVE_BID_LEVELS:
                        self.ACTIVE_BID_LEVELS.remove(price)
                else:
                    if price in self.ACTIVE_ASK_LEVELS:
                        self.ACTIVE_ASK_LEVELS.remove(price)
            elif price in self.ACTIVE_BID_LEVELS or price in self.ACTIVE_ASK_LEVELS:
                lob_action = 4  # Update level
            else:
                lob_action = 2  # Insert level
                if side == "buy":
                    self.ACTIVE_BID_LEVELS.add(price)
                else:
                    self.ACTIVE_ASK_LEVELS.add(price)

            lob_events.append(create_lob_event(
                quote_no=self.QUOTE_NO,
                event_no=self.EVENT_NO,
                side=1 if data["side"] == "buy" else 2,
                price=price,
                size=qty if qty > 0 else -1,
                lob_action=lob_action,
                send_timestamp=ts,
                # Manually appended timestamp (see websocket_manager.py)
                receive_timestamp=data["receive_timestamp"],
                order_type=0,
            ))
            self.QUOTE_NO += 1
        elif data["feed"] == "trade_snapshot":
            for trade in data["trades"]:
                self._handle_market_order(market_orders, trade)
        elif data["feed"] == "trade":
            self._handle_market_order(market_orders, data)
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

    def _handle_market_order(self, market_orders, trade):
        market_orders.append(create_market_order(
            order_id=self.ORDER_ID,
            price=trade["price"],
            trade_id=trade["uid"],
            timestamp=trade["time"],
            side=1 if trade["side"] == "buy" else 2,
            size=trade["qty"],
            msg_original_type=trade["type"]
        ))
        self.ORDER_ID += 1

