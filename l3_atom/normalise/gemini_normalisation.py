from helpers.util import create_lob_event, create_market_order
import json

class NormaliseGemini():
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def normalise(self, data) -> dict:
        lob_events = []
        market_orders = []
        valid = False
        # Handling new LOB events
        if data['type'] == 'heartbeat':
            return self.NO_EVENTS
        elif data['type'] == 'trade':
            trade = data
            valid = True
            market_orders.append(create_market_order(
                order_id=self.ORDER_ID,
                trade_id=int(trade['event_id']),
                price=float(trade['price']),
                timestamp=int(trade['timestamp']),
                side=1 if trade['side'] == 'buy' else 2,
                size=float(trade['quantity']),
                msg_original_type=data['type']
            ))
            self.ORDER_ID += 1
        if 'changes' in data:
            valid = True
            for order in data['changes']:
                side = 1 if order[0] == 'buy' else 2
                price = float(order[1])
                size = float(order[2])
                # For Gemini, if the order size is 0, it means that the level is being removed
                if size == 0:
                    lob_action = 3
                    if price in self.ACTIVE_LEVELS:
                        self.ACTIVE_LEVELS.remove(price)
                    self.QUOTE_NO += 1
                # If the price is already in the active levels, it means that the level is being updated with a new size
                elif price in self.ACTIVE_LEVELS:
                    lob_action = 4
                # Otherwise, it means that a new price level is being inserted
                else:
                    lob_action = 2
                    self.ACTIVE_LEVELS.add(price)
                # Once the nature of the lob event has been determined, it can be created and added to the list of lob events
                lob_events.append(create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=side,
                    price=price,
                    size=size,
                    lob_action=lob_action,
                    receive_timestamp=data["receive_timestamp"],
                    order_type=0
                ))
                self.QUOTE_NO += 1
                self.ORDER_ID += 1
                self.EVENT_NO += 1

        if "trades" in data:
            valid = True
            for trade in data['trades']:
                market_orders.append(create_market_order(
                    order_id=self.ORDER_ID,
                    trade_id=int(trade['event_id']),
                    price=float(trade['price']),
                    timestamp=int(trade['timestamp']),
                    side=1 if trade['side'] == 'buy' else 2,
                    size=float(trade['quantity']),
                    msg_original_type='trade'
                ))
                self.ORDER_ID += 1

        # If the data is in an unexpected format, ignore it
        if not valid:
            print(f"Received unrecognised message {json.dumps(data)}")
            return self.NO_EVENTS
        self.EVENT_NO += 1

        # Creating final normalised data dictionary which will be returned to the Normaliser
        normalised = {
            "lob_events": lob_events,
            "market_orders": market_orders
        }

        return normalised