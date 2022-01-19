from queue import PriorityQueue

class OrderBook:
    def __init__(self):
        self.sell_orders = PriorityQueue()
        self.buy_orders = PriorityQueue()
        self.best_buy_order = None
        self.best_sell_order = None

    def add_order(self, order):
        if order['side'] == 1:
            if self.best_buy_order is None:
                self.best_buy_order = order
            elif self.best_buy_order['price'] < order['price']:
                self.buy_orders.put((self.best_buy_order['price'], self.best_buy_order))
                self.best_buy_order = order
            else:
                self.buy_orders.put((order['price'], order))
        else:
            if self.best_sell_order is None:
                self.best_sell_order = order
            elif self.best_sell_order['price'] > order['price']:
                self.sell_orders.put((self.best_sell_order['price'], self.best_sell_order))
                self.best_sell_order = order
            else:
                self.sell_orders.put((order['price'], order))
        self.trades()

    def trades(self):
        if not self.best_buy_order or not self.best_sell_order:
            return
        while self.best_buy_order['price'] >= self.best_sell_order['price'] and not self.buy_orders.empty() and not self.sell_orders.empty():
            print("Trade completed between {} and {}".format(self.best_buy_order, self.best_sell_order))
            if self.best_buy_order['size'] > self.best_sell_order['size']:
                self.best_buy_order['size'] -= self.best_sell_order['size']
                self.best_sell_order = self.sell_orders.get()[1]
            elif self.best_buy_order['size'] < self.best_sell_order['size']:
                self.best_sell_order['size'] -= self.best_buy_order['size']
                self.best_buy_order = self.buy_orders.get()[1]
            else:
                self.best_buy_order = self.buy_orders.get()[1]
                self.best_sell_order = self.sell_orders.get()[1]

    def print_order_book_pretty(self):
        sells_copy = self.sell_orders.queue
        buys_copy = self.buy_orders.queue
        sells_copy.reverse()
        buys_copy.reverse()
        print('Sells:')
        while sells_copy:
            print(sells_copy.pop())
        print(self.best_sell_order)
        print('Buys:')
        while buys_copy:
            print(buys_copy.pop())
            print(self.best_buy_order)

def main():
    order_book = OrderBook()
    order_book.add_order({'price': 1.0, 'size': 1.0, 'side': 1})
    order_book.add_order({'price': 1.0, 'size': 1.0, 'side': 2})
    order_book.add_order({'price': 2.0, 'size': 3.0, 'side': 1})
    order_book.add_order({'price': 2.5, 'size': 1.0, 'side': 2})
    order_book.add_order({'price': 1.5, 'size': 1.0, 'side': 1})
    order_book.print_order_book_pretty()

if __name__ == '__main__':
    main()




    