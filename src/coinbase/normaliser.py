"""
normaliser

Mediator class which provides a wrapper for normalising data from a websocket feed.
Instantiates the correct websocket connection with an ID.
"""
from threading import Thread, Lock
from time import sleep
import os
import requests
import json
from coinbase_ws_factory import CoinbaseWsManagerFactory
from kafka_consumer import ExchangeDataConsumer
from normalised_producer import NormalisedDataProducer
from coinbase_normalisation import NormaliseCoinbase
from table import LobTable, MarketOrdersTable
from l3_order_book import L3OrderBookManager
from metrics import Metric
import time

class Normaliser():
    METRIC_CALCULATION_FREQUENCY = 100  # Times per second

    def __init__(self, exchange_id: str, symbol: str):
        self.name = exchange_id + ":" + symbol
        self.url = "wss://ws-feed.pro.coinbase.com"
        self.symbol = symbol
        # Initialise WebSocket handler
        #self.ws_manager = CoinbaseWsManagerFactory.get_ws_manager(exchange_id, symbol)
        self.consumer = ExchangeDataConsumer(symbol.replace("-", ""))
        self.producer = NormalisedDataProducer(f"test-{symbol.replace('-', '')}")
        # Retrieve correct normalisation function
        self.normalise = NormaliseCoinbase().normalise

        # Initialise tables
        self.lob_table = LobTable()
        self.market_orders_table = MarketOrdersTable()
        self.order_book_manager = L3OrderBookManager()
        # self.writer = DataWriter(exchange_id, symbol)

        # Metric observers
        self.metrics = []

        # Initialise locks
        self.lob_table_lock = Lock()
        self.metric_lock = Lock()
        self.lob_lock = Lock()

        # Start normalising the data
        self.normalise_thr = Thread(
            name="normalising_thread",
            target=self._normalise_thread,
            args=(),
            daemon=True
        )
        self.normalise_thr.start()

        # Calculate metrics once every 100ms
        self.metrics_thr = Thread(
            name="metrics_thread",
            target=self._metric_threads,
            args=(),
            daemon=True
        )
        self.metrics_thr.start()

        self.put_entry(self.get_snapshot())

    def get_snapshot(self):
        data = requests.get(f'https://api.exchange.coinbase.com/products/{self.symbol}/book?level=3')
        return data.json()

    def put_entry(self, data: dict):
        """
        Puts data into the table.

        :param data: Data to be put into the table.
        :return: None
        """
        if not data:
            return
        if isinstance(data, dict):
            data = self.normalise(data)
        else:
            data = self.normalise(json.loads(data))
        lob_events = data["lob_events"]
        market_orders = data["market_orders"]

        self.lob_table_lock.acquire()
        self.lob_lock.acquire()
        for event in lob_events:
            if len(event) == 22:
                self.order_book_manager.handle_event(event)
                event["size_ahead"], event["orders_ahead"] = self.order_book_manager.get_ahead(event)
                self.lob_table.put_dict(event)
                self.producer.produce("%s,%s,LOB" % ("Coinbase", self.url), event)
                time.sleep(0.001)
        self.lob_lock.release()
        self.lob_table_lock.release()

        for order in market_orders:
            print(order)
            self.market_orders_table.put_dict(order)
            self.producer.produce("%s,%s,TRADES" % ("Coinbase", self.url), order)

    def get_lob_events(self):
        """Returns the lob events table."""
        # NOTE: MODIFYING THE LOB TABLE AFTER RETRIEVING IT USING THIS FUNCTION IS NOT THREAD SAFE
        #       It is only safe for metrics which are observing this normaliser through the calculate_metrics method.
        return self.lob_table

    def get_best_orders(self):
        """Returns the best current bid and ask orders."""
        return self.order_book_manager.best_buy_order, self.order_book_manager.best_sell_order

    def get_market_orders(self):
        """Returns the market orders table."""
        return self.market_orders_table

    def dump(self):
        """Outputs the current state of the normaliser."""
        self._wrap_output(self._dump)()
        return

    def _dump(self):
        """Modify to change the output format."""
        #self._dump_lob_table()
        self._dump_market_orders()
        self._dump_lob()
        #self.ws_manager.get_q_size()  # Queue backlog
        self._dump_metrics()
        return

    def add_metric(self, metric: Metric):
        """Adds a metric to the normaliser."""
        if metric in self.metrics:
            return
        self.metrics.append(metric)

    def remove_metric(self, metric: Metric):
        """Removes a metric from the normaliser."""
        if not metric in self.metrics:
            return
        self.metrics.remove(metric)

    def calculate_metrics(self):
        """Calculates all the assigned metrics"""
        threads = []
        self.lob_table_lock.acquire()
        self.lob_lock.acquire()
        self.metric_lock.acquire()
        for metric in self.metrics:
            t = Thread(
                target=Metric.metric_wrapper,
                args=(metric.calculate, self, metric),
                daemon=True
            )
            threads.append(t)
            t.start()
        for thread in threads:
            t.join()
        self.metric_lock.release()
        self.lob_lock.release()
        self.lob_table_lock.release()

    def _normalise_thread(self):
        while True:
            # NOTE: This function blocks when there are no messages in the queue.
            data = self.consumer.consume()
            #print(data)
            self.put_entry(data)

    def _metric_threads(self):
        while True:
            self.calculate_metrics()
            sleep(1/self.METRIC_CALCULATION_FREQUENCY)

    def _wrap_output(self, f):
        def wrapped():
            #os.system("clear")
            print(
                f"-------------------------------------------------START {self.name}-------------------------------------------------")
            f()
            print(
                f"--------------------------------------------------END {self.name}--------------------------------------------------")
        return wrapped

    def _dump_lob_table(self):
        print("LOB Events")
        self.lob_table_lock.acquire()
        self.lob_table.dump()
        self.lob_table_lock.release()

    def _dump_market_orders(self):
        print("Market Orders")
        self.market_orders_table.dump()

    def _dump_lob(self):
        self.lob_lock.acquire()
        self.order_book_manager.dump()
        self.lob_lock.release()

    def _dump_metrics(self):
        print("-------------------------METRICS---------------------------")
        self.metric_lock.acquire()
        for metric in self.metrics:
            metric.display_metric()
        self.metric_lock.release()
        print("-----------------------------------------------------------")
