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
            key_type = types.float64,
            value_type = types.float64
        )
        self.buy_orders = Dict.empty(
            key_type = types.float64,
            value_type = types.float64
        )

        self.best_buy_order = None
        self.best_sell_order = None
    
    def handle_event(self, lob_event):
        side = lob_event['side']
        if side == 1:
            book = self.buy_orders
            best_price = self.best_buy_order['price'] if self.best_buy_order else None
        else:
            book = self.sell_orders
            best_price = self.best_sell_order['price'] if self.best_sell_order else None
        args = (best_price,) + (lob_event['price'], lob_event['side'], lob_event['size'])

        if lob_event['lob_action'] == 2:
            new_best = OrderBookManager.insert(book, *args)
        elif lob_event['lob_action'] == 3:
            new_best = OrderBookManager.delete(book, *args)
        elif lob_event['lob_action'] == 4:
            new_best = OrderBookManager.update(book, *args)

        if len(new_best) == 0:
            return

        if side == 1:
            self.best_buy_order = new_best
        else:
            self.best_sell_order = new_best

    @jit(nopython=True)
    def insert(book, best_price, price, side, size):
        """
        Inserts a new order into the order book
        :param lob_event: The data from the LOB event to insert
        :return: None
        """
        level = {"price": price, "size": size}
        new_best = Dict.empty(types.unicode_type, types.float64)
        if side == 2:
            if best_price is None or price < best_price:
                new_best = level
            book[price] = size
        elif side == 1:
            if best_price is None or price > best_price:
                new_best = level
            book[price] = size
        return new_best

    @jit(nopython=True)
    def update(book, best_price, price, side, size):
        """
        Updates an existing order in the order book
        :param lob_event: The data from the LOB event to update. Finds the order with the given price, and updates its size
        :return: None
        """
        level = {"price": price, "size": size}
        if side == 2:
            book[price] = size
            if price <= best_price:
                return level
        elif side == 1:
            book[price] = size
            if price >= best_price:
                return level
        return Dict.empty(types.unicode_type, types.float64)

    @jit(nopython=True)
    def delete(book, best, price, side, size):
        """
        Deletes an order from the order book
        :param lob_event: The data from the LOB event to delete. Finds the order with the given price in the relevant table, and deletes it
        :return: None
        """
        del book[price]

        if not price == best:
            return Dict.empty(types.unicode_type, types.float64)

        if side == 2:
            best = 10e12
            for price in list(book.keys()):
                if price < best:
                    best = price 
        elif side == 1:
            best = -1
            for price in list(book.keys()):
                if price > best:
                    best = price 
        return {'price': best, 'size': book[best]}


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




    