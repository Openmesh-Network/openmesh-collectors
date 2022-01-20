'''
Classes to calculate different metrics -- each class observes an instance of a normalizer

Classes
- Metric: Parent class for metrics
- OrderBookImbalance: Calculates the order book imbalance of the normalizer using the best bid and ask volumes
- MidPrice: Calculates the mid price of the normalizer using the best bid and ask prices
- MicroPrice: Calculates the micro price of the normalizer using the imbalance and best bid and ask prices
'''
import logging
from numba import jit


class Metric:
    def __init__(self):
        self.metric = None

    def calculate(self, normalizer):
        raise NotImplementedError()

    def display_metric(self):
        print(self.metric)
    
    @staticmethod
    def metric_wrapper(f, normalizer):
        try:
            f(normalizer)
        except ZeroDivisionError as e:
                logging.log(f"calculation failed: {e}")


class OrderBookImbalance(Metric):
    def calculate(self, normalizer):

        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return

        ask_vol = best_ask["size"]
        bid_vol = best_bid["size"]

        self.metric = (bid_vol) / (ask_vol + bid_vol)
        return self.metric

    def display_metric(self):
        if not self.metric:
            print("Order book Imbalance: Not Calculated")
            return
        print("Order book Imbalance: %.4f" % self.metric)


class MidPrice(Metric):
    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return

        self.metric = (best_bid["price"] + best_ask["price"]) / 2

        return self.metric 

    def display_metric(self):
        if not self.metric:
            print("Mid Price: Not Calculated")
            return
        print("Mid Price: $%.4f" % self.metric)


class MicroPrice(Metric):
    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        imbalance = OrderBookImbalance().calculate(normalizer)
        if not best_bid or not best_ask:
            return

        bid_price = best_bid['price']
        ask_price = best_ask['price']
        self.metric = bid_price * (1 - imbalance) + ask_price * imbalance
        return self.metric 

    def display_metric(self):
        if not self.metric:
            print("Micro Price: Not Calculated")
            return 
        print("Micro Price: $%.4f" % self.metric)


class NumberOfLOBEvents(Metric):
    def calculate(self, normalizer):
        self.metric = normalizer.get_lob_events().size()
        return self.metric

    def display_metric(self):
        if not self.metric:
            print("Number of LOB Events: Not Calculated")
            return 
        print("Number of LOB Events: %d" % self.metric)


class RatioOfLobEvents(Metric):
    def __init__(self):
        self.updates = 0
        self.inserts = 0
        self.deletes = 0
        self.updates_ratio = 0
        self.deletes_ratio = 0
        self.inserts_ratio = 0
        self.curr_row = 0

    @jit(forceobj=True)
    def calculate(self, normalizer):
        table = normalizer.get_lob_events()
        for i in range(self.curr_row,table.height):
            event = table.get_row(i)
            if event['lob_action'] == 2:
                self.inserts += 1
            elif event['lob_action'] == 4:
                self.updates += 1
            elif event['lob_action'] == 3:
                self.deletes += 1
            self.curr_row += 1
        if self.updates * self.deletes * self.inserts == 0:
            return
        self.inserts_ratio = self.inserts / (self.updates + self.deletes + self.inserts)
        self.updates_ratio = self.updates / (self.updates + self.deletes + self.inserts) 
        self.deletes_ratio = self.deletes / (self.updates + self.deletes + self.inserts)
        return self.inserts_ratio, self.updates_ratio, self.deletes_ratio

    def display_metric(self):
        if self.inserts_ratio * self.updates_ratio * self.deletes_ratio == 0:
            print("Ratio of LOB Events (inserts/updates/deletes): Not Calculated") 
            return
        print("Ratio of LOB Events (inserts/updates/deletes): %.4f/%.4f/%.4f" % 
                (self.inserts_ratio, self.updates_ratio, self.deletes_ratio))
