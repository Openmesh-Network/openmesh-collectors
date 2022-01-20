from tabulate import tabulate
import numpy as np
import sys

class OrderBookManager:
    def __init__(self, mode="min"):
        """
        Handles the buy and sell orders, storing the best for metric calculations
        """
        self.sell_orders = {}
        self.buy_orders = {}

        self.best_buy_order = None
        self.best_sell_order = None
    
    def handle_event(self, lob_event):
        if lob_event['lob_action'] == 2:
            self.insert({"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})
        elif lob_event['lob_action'] == 3:
            self.delete({"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})
        elif lob_event['lob_action'] == 4:
            self.update({"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})

    def insert(self, lob_event):
        """
        Inserts a new order into the order book
        :param lob_event: The data from the LOB event to insert
        :return: None
        """
        price = lob_event["price"]
        size = lob_event["size"]
        level = {"price": price, "size": size}
        if lob_event["side"] == 2:
            if self.best_sell_order is None or price < self.best_sell_order["price"]:
                self.best_sell_order = level
            self.sell_orders[price] = size
        elif lob_event["side"] == 1:
            if self.best_buy_order is None or price > self.best_buy_order["price"]:
                self.best_buy_order = level
            self.buy_orders[price] = size

    def update(self, lob_event):
        """
        Updates an existing order in the order book
        :param lob_event: The data from the LOB event to update. Finds the order with the given price, and updates its size
        :return: None
        """
        price = lob_event['price']
        size = lob_event['size']
        level = {"price": price, "size": size}
        if lob_event['side'] == 2:
            self.sell_orders[price] = size
            if price <= self.best_sell_order['price']:
                self.best_sell_order = level
        elif lob_event['side'] == 1:
            self.buy_orders[price] = size
            if price >= self.best_buy_order['price']:
                self.best_buy_order = level

    def delete(self, lob_event):
        """
        Deletes an order from the order book
        :param lob_event: The data from the LOB event to delete. Finds the order with the given price in the relevant table, and deletes it
        :return: None
        """
        price = lob_event['price']
        if lob_event['side'] == 2:
            del self.sell_orders[price]
            if price == self.best_sell_order['price']:
                best_price = sorted(self.sell_orders)[0]
                self.best_sell_order = {'price': best_price, 'size': self.sell_orders[best_price]}
        elif lob_event['side'] == 1:
            del self.buy_orders[price]
            if price == self.best_buy_order['price']:
                best_price = sorted(self.buy_orders, reverse=True)[0]
                self.best_buy_order = {'price': best_price, 'size': self.buy_orders[best_price]}

    def dump(self):
        """
        Prints the data in the order book in a table format
        :return: None
        """
        """
        n_rows = 10
        print("Sell Orders\n")
        dist_from_end = self.sell_orders.capacity - self.sell_orders.height
        print(tabulate(np.sort(self.sell_orders.table, order = ("price"))[dist_from_end:dist_from_end + n_rows], headers="keys", tablefmt="fancy_grid"))
        print("\n\nBuy Orders\n")
        print(tabulate(np.sort(self.buy_orders.table, order = ("price"))[:-n_rows:-1], headers="keys", tablefmt="fancy_grid"))
        """
        print("\nBEST ASK: " + str(self.best_sell_order))
        print("\nBEST BID: " + str(self.best_buy_order))
    

def main():
    """
    Simple tests for the OrderBookManager class
    """
    order_book = OrderBookManager()
    order_book.insert({"side": 1, "price": 10, "size": 10})
    order_book.insert({"side": 1, "price": 20, "size": 10})
    order_book.insert({"side": 1, "price": 30, "size": 10})
    order_book.insert({"side": 2, "price": 40, "size": 20})
    order_book.insert({"side": 2, "price": 30, "size": 20})
    order_book.delete({"side": 1, "price": 30, "size": 10})
    order_book.delete({"side": 2, "price": 30, "size": 20})
    order_book.update({"side": 1, "price": 10, "size": 20})
    order_book.dump()

if __name__ == '__main__':
    main()




    