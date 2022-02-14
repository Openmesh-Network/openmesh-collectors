from tabulate import tabulate
from numba import jit
from threading import Thread
from queue import Queue
import numpy as np

from table import OrderBookTable

class OrderBookManager:
    """
    Handles the buy and sell orders, storing the best for metric calculations
    """
    def __init__(self):
        self.sell_orders = OrderBookTable()
        self.buy_orders = OrderBookTable()

        self.sell_queue = Queue()
        self.buy_queue = Queue()

        self.best_buy_order = None
        self.best_sell_order = None

        self.sell_thread = Thread(
            name = "sell_thread",
            target = self._sell_thread,
            args = (),
            daemon = True
        )
    
        self.buy_thread = Thread(
            name = "buy_thread",
            target = self._buy_thread,
            args = (),
            daemon = True
        )

        self.sell_thread.start()
        self.buy_thread.start()

    def handle_event(self, lob_event):
        if lob_event['side'] == 1:
            self.buy_queue.put(lob_event)
        elif lob_event['side'] == 2:
            self.sell_queue.put(lob_event)
    
    def _handle_event(self, lob_event):
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
        if lob_event["side"] == 2:
            if self.best_sell_order is None or price < self.best_sell_order["price"]:
                self.best_sell_order = {"price": price, "size": size}
            self.sell_orders.put_dict({"price": price, "size": size})
        elif lob_event["side"] == 1:
            if self.best_buy_order is None or price > self.best_buy_order["price"]:
                self.best_buy_order = {"price": price, "size": size}  
            self.buy_orders.put_dict({"price": price, "size": size})

    def update(self, lob_event):
        """
        Updates an existing order in the order book
        :param lob_event: The data from the LOB event to update. Finds the order with the given price, and updates its size
        :return: None
        """
        price = lob_event['price']
        size = lob_event['size']
        if lob_event['side'] == 2:
            row = OrderBookManager._get_row_by_price(self.sell_orders.table, price)
            self.sell_orders.table[row]['size'] = size
            if price <= self.best_sell_order['price']:
                self.best_sell_order = {"price": price, "size": size}
        elif lob_event['side'] == 1:
            row = OrderBookManager._get_row_by_price(self.buy_orders.table, price)
            self.buy_orders.table[row]['size'] = size
            if price >= self.best_buy_order['price']:
                self.best_buy_order = {"price": price, "size": size}

    def delete(self, lob_event):
        """
        Deletes an order from the order book
        :param lob_event: The data from the LOB event to delete. Finds the order with the given price in the relevant table, and deletes it
        :return: None
        """
        price = lob_event['price']
        if lob_event['side'] == 2:
            row = OrderBookManager._get_row_by_price(self.sell_orders.table, price)
            if row == -1:
                return
            self.sell_orders.del_row(row)
            if price == self.best_sell_order['price']:
                price_ind = OrderBookManager._get_new_best_price(self.sell_orders.table, 2)
                self.best_sell_order = {
                    'price': self.sell_orders.table[price_ind]['price'], 
                    'size': self.sell_orders.table[price_ind]['size']
                }
        elif lob_event['side'] == 1:
            row = OrderBookManager._get_row_by_price(self.buy_orders.table, price)
            if row == -1:
                return
            self.buy_orders.del_row(row)
            if price == self.best_buy_order['price']:
                price_ind = OrderBookManager._get_new_best_price(self.buy_orders.table, 1)
                self.best_buy_order = {
                    'price': self.buy_orders.table[price_ind]['price'], 
                    'size': self.buy_orders.table[price_ind]['size']
                }

    def dump(self):
        """
        Prints the data in the order book in a table format
        :return: None
        """
        n_rows = 10
        print("Sell Orders\n")
        dist_from_end = self.sell_orders.capacity - self.sell_orders.height
        print(tabulate(np.sort(self.sell_orders.table, order = ("price"))[dist_from_end:dist_from_end + n_rows], headers="keys", tablefmt="fancy_grid"))
        print("\nBEST ASK: " + str(self.best_sell_order))
        print("\n\nBuy Orders\n")
        print(tabulate(np.sort(self.buy_orders.table, order = ("price"))[:-n_rows:-1], headers="keys", tablefmt="fancy_grid"))
        print("\nBEST BID: " + str(self.best_buy_order))
    
    def _sell_thread(self):
        while True:
            self._handle_event(self.sell_queue.get())

    def _buy_thread(self):
        while True:
            self._handle_event(self.buy_queue.get())

    @staticmethod
    @jit(nopython=True)
    def _get_row_by_price(table, price):
        """
        Given the price and side of an order, returns the index of the row in the relevant table
        :param price: The price of the order
        :param side: The side of the order
        :return: The index of the row in the relevant table
        """
        index = 0
        for order in table:
            if order["price"] == price:
                return index
            index += 1
        return -1

    @staticmethod
    @jit(nopython=True)
    def _get_new_best_price(table, side: int):
        """
        When an order is deleted, this function is called to find the new best order in the relevant table
        :param side: The side of the order to find the new best order for
        :return: The new best order
        """
        if side == 2:
            min_price_ind = -1
            min_price = 10e9 + 5
            for i in range(len(table)):
                if table[i]["price"] < min_price and table[i]["price"] > 0:
                    min_price = table[i]["price"]
                    min_price_ind = i
            return min_price_ind
        elif side == 1:
            max_price_ind = -1
            max_price = -1
            for i in range(len(table)):
                if table[i]["price"] > max_price:
                    max_price = table[i]["price"]
                    max_price_ind = i
            return max_price_ind

    

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




    