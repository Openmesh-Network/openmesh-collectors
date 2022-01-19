'''
Classes to calculate different metrics -- each class observes an instance of a normalizer

Classes
- Metric: Parent class for metrics
- OrderBookImbalance: Calculates the order book imbalance of the normalizer using the best bid and ask volumes
- MidPrice: Calculates the mid price of the normalizer using the best bid and ask prices
- MicroPrice: Calculates the micro price of the normalizer using the imbalance and best bid and ask prices
'''

class Metric:

    def calculate(self, normalizer):
        raise NotImplementedError()

class OrderBookImbalance(Metric):

    def calculate(self, normalizer):
        
        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return
        
        ask_vol = best_ask["size"]
        bid_vol = best_bid["size"]

        return "Order Book imbalance: ", (bid_vol) / (ask_vol + bid_vol)

class MidPrice(Metric):

    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return

        mid_price = (best_bid["price"] + best_ask["price"]) / 2

        return "Mid Price: $", mid_price

class MicroPrice(Metric):

    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        imbalance = OrderBookImbalance().calculate(normalizer)[1]
        micro_price = best_bid['price'] * (1 - imbalance) + best_ask['price'] * imbalance
        return "Micro Price: $", micro_price