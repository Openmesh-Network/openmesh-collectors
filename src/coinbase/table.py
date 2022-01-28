"""
table

Contains table data structures which represent the data tables specified in the L3 Atom Medium article. 
Read the Table class to get an idea of what everything does.

Classes:
- Table
- LobTable
- TimestampTable
- OrderDetailsTable
- MarketOrdersTable
"""
from numba import jit
from tabulate import tabulate
import numpy as np


class Table():

    INITIAL_CAPACITY = 10  # Initial height of array upon init.

    def __init__(self, colnames: list, dtype: list):
        """
        Table

        Data structure for building 2D database tables, implemented using numpy. This table cannot
        extend the number of columns.

        Index using the column name first then the row index.

        :param colnames: List containing the column names of the table.
        """
        if colnames is None or dtype is None:
            print(f"colnames or dtype cannot be none.")
            return
        elif len(colnames) == 0:
            return

        self.colnames = colnames
        self.dtype = np.dtype(dtype, align=True)
        self.width = len(colnames)
        self.height = 0
        self.capacity = self.INITIAL_CAPACITY
        self.table = np.zeros(self.capacity, dtype=self.dtype)

        np.set_printoptions(precision=2, suppress=True, linewidth=0)

    def size(self):
        """returns the size of the table"""
        return self.height

    @jit
    def get_cell(self, col: str, row: int):
        """
        returns the cell in the given column and row.
        :param col: Column name
        :param row: Row index
        :return: Cell value
        """
        if not col in self.colnames:
            raise IndexError(f"No such column {col} in Table.")
        elif row >= self.height:
            raise IndexError(
                f"Index {row} out of bounds (Length: {self.height}).")
        return self.table[row][col]

    @jit
    def set_cell(self, col: str, row: int, data):
        """
        Sets the cell in the given column and row.
        :param col: Column name
        :param row: Row index
        :param data: Data to set
        :return: None
        """
        if not col in self.colnames:
            raise IndexError(f"No such column {col} in Table.")
        elif row >= self.height:
            raise IndexError(
                f"Index {row} out of bounds (Length: {self.height}).")
        self.table[row][col] = data

    def get_row(self, row: int):
        """
        Returns the row at the given index.
        :param row: Row index
        :return: Row
        """
        if row >= self.height:
            raise IndexError(
                f"Index {row} out of bounds (Length: {self.height}).")
        return self.table[row]

    @jit
    def set_list(self, row: int, data: list):
        """
        Sets the row at the given index to the given list.
        :param row: Row index
        :param data: List to set
        :return: None
        """
        if row >= self.height:
            raise IndexError()
            #    f"Index {row} out of bounds (Length: {self.height}).")
        self._set_list(row, data)

    @jit
    def put_list(self, data: list):
        """
        Appends a row to the table.
        :param data: List to append
        :return: None
        """
        if self.height >= self.capacity:
            self._expand_table()

        row = self.height
        self._set_list(row, data)
        self.height += 1

    def set_dict(self, row: int, data: dict):
        """
        Sets the row at the given index to the given dictionary.
        :param row: Row index
        :param data: Dictionary to set
        :return: None
        """
        self._set_dict(row, data)

    def put_dict(self, data: dict):
        """
        Appends a dictionary to the table.
        :param data: Dictionary to append
        :return: None
        """
        if self.height >= self.capacity:
            self._expand_table()

        row = self.height
        self._set_dict(row, data)
        self.height += 1

    def del_row(self, row: int):
        """
        Deletes the row at the given index.
        :param row: Row index
        :return: None
        """
        if row >= self.height:
            raise IndexError("Index out of bounds.")

        # Delete row at index "row" from self.table along the 0th axis (0th = row, 1st = column)
        self.table = np.delete(self.table, row, 0)
        self.height -= 1
        self.capacity -= 1

    def dump(self):
        """
        Prints the table to the console in a pretty format.
        :return: None
        """
        print(tabulate(self.table[self.height-10:self.height],
              headers=self.colnames, tablefmt="fancy_grid"))
        print("\n")

    @jit
    def _set_list(self, row: int, data: list):
        if len(data) != self.width:
            raise ValueError()
            #    f"Data list width {len(data)} is not equal to table width {self.width}")
        for i in range(len(data)):
            self.table[row][i] = data[i]

    def _set_dict(self, row: int, data: dict):
        if len(data.keys()) != self.width:
            raise ValueError(
                f"Data dictionary width {len(data)} is not equal to table width {self.width}")

        for key in data.keys():
            if key not in self.colnames:
                raise KeyError(
                    f"Column name {key} not a column in this table")

        for key in self.colnames:
            self.table[row][key] = data[key]

    def _expand_table(self):
        extension = np.zeros(self.capacity, dtype=self.dtype)
        self.table = np.concatenate((self.table, extension))
        self.capacity *= 2


class TableUtil():
    """Useful methods for working with tables."""

    def create_lob_event(self,
                         quote_no=-1,
                         event_no=-1,
                         order_id=-1,
                         original_order_id=-1,
                         side=-1,
                         price=-1,
                         size=-1,
                         lob_action=0,
                         event_timestamp=-1,
                         send_timestamp=-1,
                         receive_timestamp=-1,
                         order_type=0,
                         is_implied=-1,
                         order_executed=0,
                         execution_price=-1,
                         executed_size=-1,
                         aggressor_side=-1,
                         matching_order_id=-1,
                         old_order_id=-1,
                         trade_id=-1,
                         size_ahead=-1,
                         orders_ahead=-1):
        """
        Creates a lob event dictionary.
        :param quote_no: Quote number
        :param event_no: Event number
        :param order_id: Order ID
        :param original_order_id: Original order ID
        :param side: Side (1 = bid, 2 = ask)
        :param price: Price
        :param size: Size of order at price 
        :param lob_action: LOB action (2 = insert, 3 = delete, 4 = update)
        :param event_timestamp: Event timestamp
        :param send_timestamp: timestamp of when the request was sent
        :param receive_timestamp: timestamp of when the response was received
        :param order_type: Order type (0 = unknown, 1 = limit, 2 = market)
        :param is_implied: Indicates whether this entry appears in the implied book
        :param order_executed: Indicates whether this instruction was an execution
        :param execution_price: Execution price (if instruction is an execution)
        :param executed_size: Size of execution (if instruction is an execution)
        :param aggressor_side: Side of the book that initiates a trade (0 = unknown, 1 = bid, 2 = ask)
        :param matching_order_id: if provided and if the order is an execution, the order ID of the matching order
        :param old_order_id: Order ID the instruction refers to as provided by the exchange
        :param trade_id: the ID of the trade, if available
        :param size_ahead: The total size of the orders ahead of the current order, if the lob action is an insert or update
        :param orders_ahead: The total number of orders ahead of the current order
        :return: Dictionary of lob event
        """

        return {
            "quote_no": quote_no,
            "event_no": event_no,
            "order_id": order_id,
            "original_order_id": original_order_id,
            "side": side,
            "price": price,
            "size": size,
            "lob_action": lob_action,
            "event_timestamp": event_timestamp,
            "send_timestamp": send_timestamp,
            "receive_timestamp": receive_timestamp,
            "order_type": order_type,
            "is_implied": is_implied,
            "order_executed": order_executed,
            "execution_price": execution_price,
            "executed_size": executed_size,
            "aggressor_side": aggressor_side,
            "matching_order_id": matching_order_id,
            "old_order_id": old_order_id,
            "trade_id": trade_id,
            "size_ahead": size_ahead,
            "orders_ahead": orders_ahead
        }

    def create_market_order(self,
                            order_id=-1,
                            price=-1,
                            trade_id="",
                            timestamp=-1,
                            side=-1,
                            size=-1,
                            msg_original_type=""):
        """
        Creates a market order dictionary.
        :param order_id: Order ID
        :param price: Price of the order
        :param trade_id: ID of the trade
        :param timestamp: Timestamp of the order in POSIX
        :param side: Side of the order (0 = unknown, 1 = bid, 2 = ask)
        :param msg_original_type: message type, as sent by the exchange
        """

        return {
            "order_id": order_id,
            "price": price,
            "trade_id": trade_id,
            "timestamp": timestamp,
            "side": side,
            "size": size,
            "msg_original_type": msg_original_type
        }


class LobTable(Table):
    def __init__(self):
        """
        Table object for the Limit Order Book (LOB) data, timestamp data, and order 
        details data tables specified in the L3 Atom medium article (Table 3, 4, 5).
        """
        colnames = [  # Length: 22
            "quote_no",
            "event_no",
            "order_id",
            "original_order_id",
            "side",
            "price",
            "size",
            "lob_action",
            "event_timestamp",
            "send_timestamp",
            "receive_timestamp",
            "order_type",
            "is_implied",
            "order_executed",
            "execution_price",
            "executed_size",
            "aggressor_side",
            "matching_order_id",
            "old_order_id",
            "trade_id",
            "size_ahead",
            "orders_ahead"
        ]
        types = [
            "i8",
            "i8",
            "U36",
            "i8",
            "i8",
            "f8",
            "f8",
            "i8",
            "datetime64[ms]",
            "datetime64[ms]",
            "i8",
            "i8",
            "i8",
            "i8",
            "f8",
            "i8",
            "i8",
            "U36",
            "U36",
            "i8",
            "f8",
            "f8"
        ]
        dtype = list(zip(colnames, types))
        super().__init__(colnames, dtype)


class MarketOrdersTable(Table):
    def __init__(self):
        """
        Table object for the Market Orders table specified
        in the L3 Atom medium article (Table 7).
        """
        colnames = [
            "order_id",
            "price",
            "trade_id",
            "timestamp",
            "side",
            "size",
            "msg_original_type"
        ]
        dtype = [
            ("order_id", "U36"),
            ("price", "f8"),
            ("trade_id", "i8"),
            ("timestamp", "datetime64[ms]"),
            ("side", "i8"),
            ("size", "f8"),
            ("msg_original_type", "U12")
        ]
        super().__init__(colnames, dtype)


class OrderBookTable(Table):
    def __init__(self):
        """
        Table object to hold order book data, modified by LOB events
        """
        colnames = [
            "price",
            "size",
        ]
        dtype = [
            ("price", "f8"),
            ("size", "f8"),
        ]
        super().__init__(colnames, dtype)


if __name__ == "__main__":
    np.set_printoptions(precision=2, suppress=True)
    names = ["id", "trader_id", "side", "price"]
    dtype = [
        ("id", "u8"),
        ("trader_id", "u8"),
        ("side", "u8"),
        ("price", "f8")
    ]

    table = Table(names, dtype=dtype)
    table.dump()

    data = [1, 10, 1, 40000.0]
    table.put_list(data)
    table.dump()

    data = {"id": 2, "trader_id": 20, "side": 0, "price": 39500.0}
    table.put_dict(data)
    table.dump()

    data = [3, 30, 1, 41000.0]
    table.put_list(data)
    data = [4, 40, 1, 42000.0]
    table.put_list(data)
    table.dump()

    print(table.get_cell("trader_id", 3))
    print(table.get_row(3))

    table.set_cell("side", 2, 0)
    table.dump()

    data = [3, 50, 1, 42000.0]
    table.set_list(2, data)
    table.dump()

    data = {"id": 2, "trader_id": 60, "side": 0, "price": 39250.0}
    table.set_dict(1, data)
    table.dump()

    table.del_row(1)
    table.dump()
