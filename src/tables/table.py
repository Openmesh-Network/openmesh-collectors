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
import numpy as np

class Table():

    INITIAL_CAPACITY = 10 # Initial height of array upon init.

    def __init__(self, colnames: list):
        """
        Table

        Data structure for building 2D database tables, implemented using numpy. This table cannot
        extend the number of columns.

        Index using the column name first then the row index.

        :param colnames: List containing the column names of the table.
        """
        if colnames is None:
            return
        elif len(colnames) == 0:
            return
        
        self.colnames = colnames
        self.width = len(colnames)
        self.height = 0
        self.capacity = self.INITIAL_CAPACITY
        self.col_map = {}
        for ind, name in enumerate(colnames):
            self.col_map[name] = ind

        self.table = np.zeros((self.capacity, self.width), dtype=None)
    
    def get_cell(self, col: str, row: int):
        if not col in self.colnames:
            raise IndexError(f"No such column {col} in Table.")
        elif row >= self.height:
            raise IndexError(f"Index {row} out of bounds (Length: {self.height}).")
        return self.table[row, self.col_map[col]]
    
    def set_cell(self, col: str, row: int, data):
        if not col in self.colnames:
            raise IndexError(f"No such column {col} in Table.")
        elif row >= self.height:
            raise IndexError(f"Index {row} out of bounds (Length: {self.height}).")
        self.table[row, self.col_map[col]] = data
    
    def get_row(self, row: int):
        if row >= self.height:
            raise IndexError(f"Index {row} out of bounds (Length: {self.height}).")
        return self.table[row]

    def set_list(self, row: int, data: list):
        if row >= self.height:
            raise IndexError(f"Index {row} out of bounds (Length: {self.height}).")
        self._set_list(row, data)
    
    def put_list(self, data: list):
        if self.height >= self.capacity:
            self._expand_table()

        row = self.height
        self._set_list(row, data)
        self.height += 1
    
    def set_dict(self, row: int, data: dict):
        self._set_dict(row, data)
    
    def put_dict(self, data: dict):
        if self.height >= self.capacity:
            self._expand_table()

        row = self.height
        self._set_dict(row, data)
        self.height += 1
    
    def del_row(self, row: int):
        if row >= self.height:
            raise IndexError(f"Index {row} out of bounds (Length: {self.height}).")
        
        # Delete row at index "row" from self.table along the 0th axis (0th = row, 1st = column)
        self.table = np.delete(self.table, row, 0)
        self.height -= 1
        self.capacity -= 1
    
    def print_table(self):
        print(self.table)
        print("\n")
    
    def _set_list(self, row: int, data: list):
        if len(data) != self.width:
            raise ValueError(f"Data list width {len(data)} is not equal to table width {self.width}")
        for i in range(len(data)):
            self.table[row, i] = data[i]
    
    def _set_dict(self, row: int, data: dict):
        if len(data.keys()) != self.width:
            raise ValueError(f"Data dictionary width {len(data)} is not equal to table width {self.width}")
        
        for key in data.keys():
            if key not in self.colnames:
                raise KeyError(f"Column name {key} not a column in this table")

        for key in self.colnames:
            self.table[row, self.col_map[key]] = data[key]
             
    def _expand_table(self):
        extension = np.zeros((self.capacity, self.width), dtype = None)
        self.table = np.concatenate((self.table, extension))
        self.capacity *= 2


class LobTable(Table):
    def __init__(self):
        """
        Table object for the Limit Order Book (LOB) data table specified
        in the L3 Atom medium article (Table 3).
        """
        colnames = [
            "quote_no",
            "event_no",
            "order_id",
            "original_order_id",
            "side",
            "price",
            "size",
            "lob_action"
        ]
        super().__init__(colnames)


class TimestampTable(Table):
    def __init__(self):
        """
        Table object for the Timestamp Data table specified
        in the L3 Atom medium article (Table 4).
        """
        colnames = [
            "event_timestamp",
            "send_timestamp",
            "receive_timestamp"
        ]
        super().__init__(colnames)


class OrderDetailsTable(Table):
    def __init__(self):
        """
        Table object for the Order Details data table specified
        in the L3 Atom medium article (Table 5).
        """
        colnames = [
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
        super().__init__(colnames)


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
            "msg_original_type"
        ]
        super().__init__(colnames)


if __name__ == "__main__":
    np.set_printoptions(precision=2, suppress=True)

    table = Table(["id", "trader_id", "side", "price"])
    table.print_table()

    data = [1, 10, 1, 40000.0]
    table.put_list(data)
    table.print_table()

    data = {"id": 2, "trader_id": 20, "side": 0, "price": 39500.0}
    table.put_dict(data)
    table.print_table()

    data = [3, 30, 1, 41000.0]
    table.put_list(data)
    data = [4, 40, 1, 42000.0]
    table.put_list(data)
    table.print_table()

    print(table.get_cell("trader_id", 3))
    print(table.get_row(3))

    table.set_cell("side", 2, 0)
    table.print_table()

    data = [3, 50, 1, 42000.0]
    table.set_list(2, data)
    table.print_table()

    data = {"id": 2, "trader_id": 60, "side": 0, "price": 39250.0}
    table.set_dict(1, data)
    table.print_table()

    table.del_row(1)
    table.print_table()