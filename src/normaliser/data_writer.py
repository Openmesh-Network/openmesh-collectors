from .tables.table import LobTable, MarketOrdersTable

class DataWriter():
    def __init__(self, exchange_id, symbol):
        self.market_orders = open("market_orders_" + exchange_id + "_" + symbol + ".csv", "w")
        self.lob_events = open("lob_events_" + exchange_id + "_" + symbol + ".csv", "w")

        self.market_orders.write(",".join(MarketOrdersTable().colnames) + "\n")
        self.lob_events.write(",".join(LobTable().colnames) + "\n")

        self.market_orders.flush()
        self.lob_events.flush()
    
    def write_lob_event(self, event):
        self.lob_events.write(self._csv_line(event))
        self.lob_events.flush()
    
    def write_market_order(self, order):
        self.market_orders.write(self._csv_line(order))
        self.market_orders.flush()
    
    def _csv_line(self, d):
        val = list(d.values())
        for i in range(len(val)):
            if isinstance(val[i], str):
                val[i] = '"' + val[i] + '"'
        return ",".join(list(map(str, val))) + "\n"