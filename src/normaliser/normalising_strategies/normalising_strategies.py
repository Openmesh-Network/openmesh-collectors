"""
normalising_strategies

Normalising strategies for each exchange. Takes input from data feeds and normalises them into 
a form suitable to be put into the data tables.
"""
import time
import json
from typing import Callable

from src.normaliser.tables.table import TableUtil


class NormalisingStrategies():
    def __init__(self):
        self.strategies = {}
        self.register()

    def register(self):
        self.strategies["kraken"] = NormaliseKraken()
        self.strategies["okex"] = NormaliseOkex()
        self.strategies["phemex"] = NormalisePhemex()
        self.strategies["huobi"] = NormaliseHuobi()
        self.strategies["ftx"] = NormaliseFtx()
        self.strategies["kucoin"] = NormaliseKucoin()

    def get_strategy(self, exchange_id) -> Callable:
        if not exchange_id in self.strategies.keys():
            raise KeyError(
                f"exchange id {exchange_id} not registered as a strategy")
        return self.strategies[exchange_id].normalise


class NormaliseExchange():
    """
    Abstract class for strategy design pattern implementation.
    """

    def normalise(self, data) -> dict:
        """
        Normalise the raw websocket dictionary data into a format acceptable by 
        the data tables.

        You can add any other private method to your implementation of the class.
        Make sure that normalise is the ONLY public method in your implementation.
        (Prefix private methods with an underscore).

        Returns a dictionary with the keys "lob_event" and "market_orders", which are 
        lists containing dictionaries suitable to be passed into the normalised tables:
        e.g: 
            normalised = {
                "lob_events" = [{<event>}, {<event>}, ...],
                "market_orders" = [{<order>}, {<order>}, ...]
            }
        """
        raise NotImplementedError()


class NormaliseKraken(NormaliseExchange):
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_BID_LEVELS = set()
    ACTIVE_ASK_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        # Useful utility functions for quickly creating table entries
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Rayman"""

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
                lob_events.append(self.util.create_lob_event(
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
                lob_events.append(self.util.create_lob_event(
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
                    self.ACTIVE_BID_LEVELS.remove(price)
                else:
                    self.ACTIVE_ASK_LEVELS.remove(price)
            elif price in self.ACTIVE_BID_LEVELS or price in self.ACTIVE_ASK_LEVELS:
                lob_action = 4  # Update level
            else:
                lob_action = 2  # Insert level
                if side == "buy":
                    self.ACTIVE_BID_LEVELS.add(price)
                else:
                    self.ACTIVE_ASK_LEVELS.add(price)

            lob_events.append(self.util.create_lob_event(
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
        market_orders.append(self.util.create_market_order(
            order_id=self.ORDER_ID,
            price=trade["price"],
            trade_id=trade["uid"],
            timestamp=trade["time"],
            side=1 if trade["side"] == "buy" else 2,
            msg_original_type=trade["type"]
        ))
        self.ORDER_ID += 1


class NormaliseOkex(NormaliseExchange):

    """
    Normaliser class for the Okex exchange
    """

    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "event" key.
        if 'event' in data:
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        if data['arg']['channel'] == 'books':
            order_data = data['data'][0]
            ts = float(order_data['ts'])
            for ask in order_data['asks']:
                price = float(ask[0])
                no_orders = int(ask[3])
                size = float(ask[1])
                # For Okex, if the number of orders is 0, it means that the level is being removed
                if no_orders == 0:
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
            for bid in order_data['bids']:
                price = float(bid[0])
                no_orders = int(bid[3])
                size = float(bid[1])
                if no_orders == 0:
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

        elif data['arg']['channel'] == 'trades':
            trade = data['data'][0]
            # Okex only sends one trade per message, so we can simply extract the data and create a market order
            market_orders.append(self.util.create_market_order(
                order_id=self.ORDER_ID,
                price=float(trade['px']),
                trade_id=trade['tradeId'],
                timestamp=float(trade['ts']),
                side=1 if trade['side'] == 'buy' else 2
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


class NormalisePhemex(NormaliseExchange):

    """
    Phemex sends prices and quantities in a "scaled" format, however they don't specify how exactly this scaling is performed, so the quantities aren't exact values
    """

    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Will"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "result" key.
        if 'result' in data:
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        if "book" in data:
            ts = float(data['timestamp'])
            order_data = data['book']
            for ask in order_data['asks']:
                price = float(ask[0])
                size = float(ask[1])
                # For Phemex, if the order size is 0, it means that the level is being removed
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

        elif "trades" in data:
            msg = data['type']
            for trade in data['trades']:
                market_orders.append(self.util.create_market_order(
                    order_id=self.ORDER_ID,
                    price=float(trade[2]),
                    timestamp=float(trade[0]),
                    side=1 if trade[1] == 'Buy' else 2,
                    msg_original_type=msg
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


class NormaliseHuobi(NormaliseExchange):
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "type" key.
        if 'subbed' in data:
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        #print(json.dumps(data, indent=4))
        if 'asks' in data['tick']:
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
                    self.QUOTE_NO += 1
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

        elif "data" in data['tick']:
            trades = data['tick']['data']
            for trade in trades:
                market_orders.append(self.util.create_market_order(
                    order_id=self.ORDER_ID,
                    trade_id=trade['tradeId'],
                    price=float(trade['price']),
                    timestamp=float(trade['ts']),
                    side=1 if trade['direction'] == 'buy' else 2,
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


class NormaliseFtx(NormaliseExchange):
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "type" key.
        if data['type'] == 'subscribed':
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        #print(json.dumps(data, indent=4))
        if 'asks' in data['data']:
            ts = float(data['data']['time'])
            order_data = data['data']
            for ask in order_data['asks']:
                price = float(ask[0])
                size = float(ask[1])
                # For FTX, if the order size is 0, it means that the level is being removed
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

        elif "liquidation" in data['data']:
            trade = data['result']
            market_orders.append(self.util.create_market_order(
                order_id=self.ORDER_ID,
                trade_id=trade['id'],
                price=float(trade['price']),
                timestamp=float(trade['time']),
                side=1 if trade[1] == 'Buy' else 2,
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


class NormaliseKucoin(NormaliseExchange):
    NO_EVENTS = {"lob_events": [], "market_orders": []}
    ACTIVE_LEVELS = set()
    QUOTE_NO = 2
    EVENT_NO = 0
    ORDER_ID = 0

    def __init__(self):
        self.util = TableUtil()

    def normalise(self, data) -> dict:
        """Jay"""
        lob_events = []
        market_orders = []

        # If the message is not a trade or a book update, ignore it. This can be seen by if the JSON response contains an "type" key.
        if data['type'] == 'ack' or data['type'] == 'welcome':
            print(f"Received message {json.dumps(data)}")
            return self.NO_EVENTS

        # Handling new LOB events
        if data['subject'] == "trade.l2update":
            order_data = data['data']['changes']
            for ask in order_data['asks']:
                price = float(ask[0])
                if price == 0:
                    return self.NO_EVENTS
                size = float(ask[1])
                ts = float(data['data']['sequenceStart'])
                # For Kucoin, if the order size is 0, it means that the level is being removed
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
                lob_events.append(self.util.create_lob_event(
                    quote_no=self.QUOTE_NO,
                    event_no=self.EVENT_NO,
                    order_id=self.ORDER_ID,
                    side=2,
                    price=price,
                    size=size if size else -1,
                    lob_action=lob_action,
                    receive_timestamp=data['receive_timestamp'],
                    order_type=0
                ))
                self.QUOTE_NO += 1
                self.ORDER_ID += 1
            for bid in order_data['bids']:
                price = float(bid[0])
                if price == 0:
                    return self.NO_EVENTS
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
                    receive_timestamp=data['receive_timestamp'],
                    order_type=0
                ))
                self.QUOTE_NO += 1
                self.ORDER_ID += 1

        elif data['subject'] == "trade.l3match":
            trade = data['data']
            market_orders.append(self.util.create_market_order(
                order_id=self.ORDER_ID,
                trade_id=trade['tradeId'],
                price=float(trade['price']),
                timestamp=float(trade['time']),
                side=1 if trade['side'] == 'buy' else 2,
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
