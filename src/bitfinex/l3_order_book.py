from tabulate import tabulate

import os
from math import fsum

class L3OrderBookManager:
    def __init__(self):
        self.buys = dict() # {price: {size, [order, order, ...]}}
        self.sells = dict() # {price: [order, order, ...]}

        self.orders = dict() # {order_id: order}

        self.best_buy_order = None
        self.best_sell_order = None
    
    def handle_event(self, lob_event):
        if lob_event['lob_action'] == 2:
            self._insert(lob_event)
        elif lob_event['lob_action'] == 3:
            self._delete(lob_event)
        elif lob_event['lob_action'] == 4:
            self._update(lob_event)

    def get_ahead(self, lob_event):
        order_id = lob_event["order_id"]
        if order_id not in self.orders.keys():
            # raise KeyError(f"order_id {order_id} not an active order.")
            return -1, -1
        
        orders_ahead = 0
        size_ahead = 0
        book = self.buys if lob_event["side"] == 1 else self.sells
        order_price = lob_event["price"]
        for price in book.keys():
            if book == self.buys and order_price < price or \
                    book == self.sells and order_price > price: 
                orders_ahead += len(book[price])
                size_ahead += self._get_level_size(price, book)
            elif order_price == price:
                i = 0
                order = book[price][i]
                while order["order_id"] != order_id:
                    orders_ahead += 1
                    size_ahead += order["size"]
                    i += 1
                    order = book[price][i]
        return size_ahead, orders_ahead

    def dump(self):
        nrows = 10
        headers = ["Price", "Size"]

        n_indexed = 0
        bids = []
        for price in sorted(self.buys.keys(), reverse=True):
            bids.append((price, self._get_level_size(price, self.buys)))
            n_indexed += 1
            if n_indexed == nrows:
                n_indexed = 0
                break

        asks = []
        for price in sorted(self.sells.keys()):
            asks.append((price, self._get_level_size(price, self.sells)))
            n_indexed += 1
            if n_indexed == nrows:
                n_indexed = 0
                break
        asks.reverse()

        print("ASKS")
        print(tabulate(asks[:nrows], headers=headers, tablefmt="fancy_grid"))

        print("BIDS")
        print(tabulate(bids[:nrows], headers=headers, tablefmt="fancy_grid"))

        print("\nBEST ASK: " + str(self.best_sell_order))
        print("\nBEST BID: " + str(self.best_buy_order))
    
    def _insert(self, lob_event):
        order_id = lob_event["order_id"]
        price = lob_event["price"]
        side = lob_event["side"]

        order_book = self.buys if side == 1 else self.sells
        if price not in order_book.keys():
            order_book[price] = [lob_event] # New price level with list
        else:
            order_book[price].append(lob_event) # Existing price levels
        self.orders[order_id] = lob_event

        if not (self.best_buy_order and self.best_sell_order) or \
                (side == 1 and price >= self.best_buy_order["price"]) or \
                (side == 2 and price <= self.best_sell_order["price"]):
            self._update_best_levels(side)

    def _delete(self, lob_event):
        order_id = lob_event["order_id"]
        if order_id not in self.orders.keys():
            # raise KeyError(f"order_id {order_id} not an active order.")
            return

        order = self.orders[order_id]
        price = order["price"]
        side = order["side"]

        order_book = self.buys if side == 1 else self.sells
        if price not in order_book.keys():
            raise KeyError(f"Deleting order at price {price} when price is not in orderbook.")
        self._pop_order(order_book, price, order_id)
        del self.orders[order_id]

        if (side == 1 and price == self.best_buy_order["price"]) or \
                (side == 2 and price == self.best_sell_order["price"]):
            self._update_best_levels(side)

    def _update(self, lob_event):
        order_id = lob_event["order_id"]
        new_price = lob_event["price"]
        new_side = lob_event["side"]
        new_size = lob_event["size"]
        if order_id not in self.orders.keys():
            raise KeyError(f"order_id {order_id} not an active order.")
        order = self.orders[order_id]
        old_price = order["price"]

        order_book = self.buys if order["side"] == 1 else self.sells

        # Don't reset queue position if new size is lower than previous size
        if new_price == old_price and order["size"] > new_size:
            order["size"] = new_size
            if (new_side == 1 and old_price == self.best_buy_order["price"]) or \
                    (new_side == 2 and old_price == self.best_sell_order["price"]):
                self._update_best_levels(new_side)
            return

        # Reset order position in queue
        order = self._pop_order(order_book, order["price"], order_id)

        if order["side"] != lob_event["side"]:
            raise ValueError("Need to implement side updates")

        order["price"] = new_price
        order["side"] = new_side
        order["size"] = lob_event["size"]

        # Place order into correct new_priceg level if price was changed
        if new_price not in order_book.keys():
            order_book[new_price] = [order]
        else:
            order_book[new_price].append(order)
        self.orders[order_id] = order
        if (new_side == 1 and old_price == self.best_buy_order["price"]) or \
                (new_side == 2 and old_price == self.best_sell_order["price"]):
            self._update_best_levels(new_side)
    
    def _pop_order(self, book, price, order_id):
        for i in range(len(book[price])):
            order = book[price][i]
            if order_id == order["order_id"]:
                book[price].pop(i)
                if len(book[price]) == 0:
                    del book[price]
                return order
        raise KeyError(f"order_id {order_id} does not exist in the orderbook.")
    
    def _get_level_size(self, price, book):
            return fsum(order["size"] for order in book[price])

    def _update_best_levels(self, side):
        book = self.buys if side == 1 else self.sells
        if len(book.keys()) == 0:
            return
        best_price = max(book.keys()) if side == 1 else min(book.keys())
        best_size = self._get_level_size(best_price, book)

        if side == 1:
            self.best_buy_order = {"price": best_price, "size": best_size}
        else:
            self.best_sell_order = {"price": best_price, "size": best_size}

def main():
    """
    Simple tests for the L3OrderBookManager class
    """
    os.system("cls")
    print("------------ORDERBOOK TEST------------")
    order_book = L3OrderBookManager()

    # Testing insertion
    order1 = create_order(1, 100, 10, 1, 2)
    order2 = create_order(2, 100, 20, 1, 2)
    order3 = create_order(3, 100, 30, 1, 2)
    order4 = create_order(4, 100, 40, 1, 2)
    order5 = create_order(5, 100, 50, 1, 2)
    orders = [order1, order2, order3, order4, order5]
    for order in orders:
        order_book.handle_event(order)
    order_book.dump()

    # Testing deletion
    order = create_order(3, 0, 0, 1, 3)
    order_book.handle_event(order)
    order_book.dump()

    # Testing update
    order = create_order(1, 100, 40, 1, 4)
    order_book.handle_event(order)
    order_book.dump()

    # Testing empty level
    order = create_order(6, 200, 40, 1, 2)
    order_book.handle_event(order)
    order_book.dump()
    order = create_order(6, 50, 0, 1, 3)
    order_book.handle_event(order)
    order_book.dump()
    print("----------END ORDERBOOK TEST----------")

def create_order(order_id, price, size, side, lob_action):
    return {
        "order_id": order_id,
        "price": price,
        "size": size,
        "side": side,
        "lob_action": lob_action
    }

if __name__ == '__main__':
    main()