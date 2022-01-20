from msvcrt import kbhit
from tabulate import tabulate
from numba import jit, literally
from numba.typed import Dict
from numba.core import types


class OrderBookManager:
    def __init__(self):
        """
        Handles the buy and sell orders, storing the best for metric calculations
        """
        self.sell_orders = Dict.empty(
            key_type = types.unicode_type,
            value_type = types.float64
        )
        self.buy_orders = Dict.empty(
            key_type = types.unicode_type,
            value_type = types.float64
        )

        self.best_buy_order = None
        self.best_sell_order = None
    
    def handle_event(self, lob_event):
        side = lob_event['side']
        if side == 1:
            book = self.buy_orders
            order = self.best_buy_order
        else:
            book = self.sell_orders
            order = self.best_sell_order

        if lob_event['lob_action'] == 2:
            new_best = self.insert(
                book,
                order,
                {"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})
        elif lob_event['lob_action'] == 3:
            new_best = self.delete(
                book,
                order,
                {"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})
        elif lob_event['lob_action'] == 4:
            new_best = self.update(
                book,
                order,
                {"price" : lob_event['price'], "size" : lob_event['size'], "side" : lob_event['side']})

        if not new_best:
            return

        if side == 1:
            self.best_buy_order = new_best
        else:
            self.best_sell_order = new_best

    @staticmethod
    @jit(nopython=True)
    def insert(book: dict, best: dict, lob_event: dict):
        """
        Inserts a new order into the order book
        :param lob_event: The data from the LOB event to insert
        :return: None
        """
        price = lob_event["price"]
        size = lob_event["size"]
        level = {"price": price, "size": size}
        new_best = None
        if lob_event["side"] == 2:
            if best is None or price < best["price"]:
                new_best = level
            book[price] = size
        elif lob_event["side"] == 1:
            if best is None or price > best["price"]:
                new_best = level
            book[price] = size
        return new_best

    @staticmethod
    @jit(nopython=True)
    def update(book, best, lob_event):
        """
        Updates an existing order in the order book
        :param lob_event: The data from the LOB event to update. Finds the order with the given price, and updates its size
        :return: None
        """
        price = lob_event['price']
        size = lob_event['size']
        level = {"price": price, "size": size}
        if lob_event['side'] == 2:
            book[price] = size
            if price <= best['price']:
                return level
        elif lob_event['side'] == 1:
            book[price] = size
            if price >= best['price']:
                return level
        return None

    @staticmethod
    @jit(nopython=True)
    def delete(book, best, lob_event):
        """
        Deletes an order from the order book
        :param lob_event: The data from the LOB event to delete. Finds the order with the given price in the relevant table, and deletes it
        :return: None
        """
        price = lob_event['price']
        del book[price]

        if not price == best['price']:
            return None

        if lob_event['side'] == 2:
            best_price = sorted(book)[0]
        elif lob_event['side'] == 1:
            best_price = sorted(book, reverse=True)[0]
        return {'price': best_price, 'size': book[best_price]}

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




    