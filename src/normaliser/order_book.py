from .tables.table import OrderBookTable
from tabulate import tabulate
import numpy as np

class OrderBookManager:
    """
    Handles the buy and sell orders, storing the best for metric calculations
    """
    def __init__(self):
        self.sell_orders = OrderBookTable()
        self.buy_orders = OrderBookTable()
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
        row = self._get_row_by_price(price, lob_event['side'])
        if row == -1:
            return
        if lob_event['side'] == 2:
            self.sell_orders.table[row]['size'] = size
            if price < self.best_sell_order['price']:
                self.best_sell_order = {"price": price, "size": size}
        elif lob_event['side'] == 1:
            self.buy_orders.table[row]['size'] = size
            if price > self.best_buy_order['price']:
                self.best_buy_order = {"price": price, "size": size}
                

    def delete(self, lob_event):
        """
        Deletes an order from the order book
        :param lob_event: The data from the LOB event to delete. Finds the order with the given price in the relevant table, and deletes it
        :return: None
        """
        price = lob_event['price']
        row = self._get_row_by_price(price, lob_event['side'])
        if row == -1:
            return
        if lob_event['side'] == 2:
            self.sell_orders.del_row(row)
            if price == self.best_sell_order['price']:
                self.best_sell_order = self._get_new_best_order(1)
        if lob_event['side'] == 1:
            self.buy_orders.del_row(row)
            if price == self.best_buy_order['price']:
                self.best_buy_order = self._get_new_best_order(2)


    def _get_row_by_price(self, price, side):
        """
        Given the price and side of an order, returns the index of the row in the relevant table
        :param price: The price of the order
        :param side: The side of the order
        :return: The index of the row in the relevant table
        """
        index = 0
        if side == 2:
            for order in self.sell_orders.table:
                if order["price"] == price:
                    return index
                index += 1
        elif side == 1:
            for order in self.buy_orders.table:
                if order["price"] == price:
                    return index
                index += 1
        return -1

    def _get_new_best_order(self, side):
        """
        When an order is deleted, this function is called to find the new best order in the relevant table
        :param side: The side of the order to find the new best order for
        :return: The new best order
        """
        if side == 2:
            min_price = 10e9 + 5
            for order in self.sell_orders.table:
                if order["price"] < min_price and order["price"] > 0:
                    min_price = order["price"]
                    return {"price": min_price, "size": order["size"]}
        elif side == 1:
            max_price = -1
            for order in self.buy_orders.table:
                if order["price"] > max_price:
                    max_price = order["price"]
                    return {"price": max_price, "size": order["size"]}

    def dump(self):
        """
        Prints the data in the order book in a table format
        :return: None
        """
        print("Sell Orders\n")
        dist_from_end = self.sell_orders.capacity - self.sell_orders.height
        print(tabulate(np.sort(self.sell_orders.table, order = ("price"))[dist_from_end:dist_from_end + 21], headers="keys", tablefmt="fancy_grid"))
        print("\nBEST ASK: " + str(self.best_sell_order))
        print("\n\nBuy Orders\n")
        print(tabulate(np.sort(self.buy_orders.table, order = ("price"))[:-21:-1], headers="keys", tablefmt="fancy_grid"))
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




    