'''
Classes to calculate different metrics -- each class observes an instance of a normalizer

Classes
- Metric: Parent class for metrics
- OrderBookImbalance: Calculates the order book imbalance of the normalizer using the best bid and ask volumes
- MidPrice: Calculates the mid price of the normalizer using the best bid and ask prices
- MicroPrice: Calculates the micro price of the normalizer using the imbalance and best bid and ask prices
'''
import logging
import numpy as np


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
        self.metric = len(normalizer.get_lob_events().table)
        return self.metric

    def display_metric(self):
        if not self.metric:
            print("Number of LOB Events: Not Calculated")
            return 
        print("Number of LOB Events: %d" % self.metric)

class RatioOfLobEvents(Metric):
    def __init__(self):
        self.updates = None
        self.inserts = None
        self.deletes = None

    def calculate(self, normalizer):
        num_updates = num_deletes = num_inserts = 0
        for event in normalizer.get_lob_events().table:
            if event['lob_action'] == 2:
                num_inserts += 1
            elif event['lob_action'] == 4:
                num_updates += 1
            elif event['lob_action'] == 3:
                num_deletes += 1
        self.inserts = num_inserts / (num_updates + num_deletes + num_inserts)
        self.updates = num_updates / (num_updates + num_deletes + num_inserts) 
        self.deletes = num_deletes / (num_updates + num_deletes + num_inserts)
        return self.inserts, self.updates, self.deletes

    def display_metric(self):
        if (not self.inserts) or (not self.updates) or (not self.deletes):
            print("Ratio of LOB Events (inserts/updates/deletes): Not Calclated") 
            return np.nan
        print("Ratio of LOB Events (inserts/updates/deletes): %.4f/%.4f/%.4f" % (self.inserts, self.updates, self.deletes))