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

    def display_metric(self):
        raise NotImplementedError()


class OrderBookImbalance(Metric):

    def calculate(self, normalizer):

        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return

        ask_vol = best_ask["size"]
        bid_vol = best_bid["size"]

        return (bid_vol) / (ask_vol + bid_vol)

    def display_metric(self, normalizer):
        return "Order book Imbalance: %.4f" % self.calculate(normalizer)


class MidPrice(Metric):

    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        if best_bid is None or best_ask is None:
            return

        mid_price = (best_bid["price"] + best_ask["price"]) / 2

        return mid_price

    def display_metric(self, normalizer):
        return "Mid Price: %.4f" % self.calculate(normalizer)

class MicroPrice(Metric):

    def calculate(self, normalizer):
        best_bid, best_ask = normalizer.get_best_orders()
        imbalance = OrderBookImbalance().calculate(normalizer)
        micro_price = best_bid['price'] * \
            (1 - imbalance) + best_ask['price'] * imbalance
        return micro_price

    def display_metric(self, normalizer):
        return "Micro Price: %.4f" % self.calculate(normalizer)
        


class NumberOfLOBEvents(Metric):

    def calculate(self, normalizer):
        return len(normalizer.get_lob_events().table)

    def display_metric(self, normalizer):
        return "Number of LOB Events: %d" % self.calculate(normalizer)

class RatioOfLobEvents(Metric):
    
    def calculate(self, normalizer):
        num_updates = num_deletes = num_inserts = 0
        for event in normalizer.get_lob_events().table:
            if event['lob_action'] == 2:
                num_inserts += 1
            elif event['lob_action'] == 4:
                num_updates += 1
            elif event['lob_action'] == 3:
                num_deletes += 1
        return num_inserts / (num_updates + num_deletes + num_inserts), num_updates / (num_updates + num_deletes + num_inserts), num_deletes / (num_updates + num_deletes + num_inserts)

    def display_metric(self, normalizer):
        return "Ratio of LOB Events (inserts/updates/deletes): %.4f/%.4f/%.4f" % self.calculate(normalizer)

                