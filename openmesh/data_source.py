from abc import abstractmethod
from typing import Union
from openmesh.helpers.enrich_data import enrich_raw
from datetime import datetime as dt
import asyncio
import requests
import uvloop
from yapic import json

from openmesh.feed import AsyncConnectionManager, AsyncFeed, WSConnection
from openmesh.sink_connector.kafka_multiprocessed import KafkaConnector

import logging

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class DataSource:
    """
    Class to manage methods dealing with an individual exchange. Stores metadata relating to symbols, endpoints, channels, e.t.c.

    :param name: Name of the exchange
    :type name: str
    :param key_field: The field in the message that will be used as the key for the Kafka message. If a string, is used as a dictionary key. If an int, is used as an index in a list
    :type key_field: Union[str, int]
    :param ws_endpoints: Dictionary of websocket endpoints and the feeds they support
    :type ws_endpoints: dict
    :param ws_channels: Dictionary of standardised websocket feeds to the exchange formats
    :type ws_channels: dict
    :param symbols_endpoint: Endpoint to get a list of symbols from
    :type symbols_endpoint: str
    :param rest_endpoints: Dictionary of REST endpoints and the feeds they support
    :type rest_endpoints: dict
    :param rest_channels: Dictionary of standardised REST feeds to the exchange formats
    :type rest_channels: dict
    """
    name = NotImplemented
    sym_field = NotImplemented
    type_field = NotImplemented
    """
    {
        <WSEndpoint>: [channels to subscribe to],
        <WSEndpoint>: [channels to subscribe to],
        ...
    }
    """
    ws_endpoints: dict = NotImplemented
    rest_endpoints: dict = NotImplemented
    symbols_endpoint: Union[str, list] = NotImplemented
    ws_channels: dict = {}
    rest_channels: dict = {}

    def __init__(self, symbols=None):
        sym_list = self.get_symbols()
        selected_syms = symbols if symbols else []
        for sym in selected_syms:
            logging.info(f"{self.name} - using symbol {sym}")
        self.symbols = self.normalise_symbols(sym_list)
        if self.symbols:
            self.inv_symbols = {v: k for k, v in self.symbols.items()}
        if selected_syms:
            self.filter_symbols(self.symbols, selected_syms)

    def get_symbols(self) -> list:
        """
        Gets a list of symbols from the exchange via the provided symbols endpoint

        :return: List of symbols
        :rtype: list
        """
        if isinstance(self.symbols_endpoint, str):
            return requests.get(self.symbols_endpoint).json()
        elif isinstance(self.symbols_endpoint, list):
            res = []
            for endpoint in self.symbols_endpoint:
                res.append(requests.get(endpoint).json())
            return res

    def filter_symbols(self, sym_list: dict, filters: dict) -> dict:
        """
        Filters a list of symbols based on the provided filters

        :param sym_list: Dictionary of symbols to filter
        :type sym_list: dict
        :param filters: Dictionary of filters to apply
        :type filters: dict
        :return: Filtered dictionary of symbols
        :rtype: dict
        """
        ret = {}
        for norm in filters:
            ret[self.get_normalised_symbol(sym_list[norm])] = sym_list[norm]
        self.symbols = ret
        self.inv_symbols = {v: k for k, v in self.symbols.items()}

    @abstractmethod
    def normalise_symbols(self, symbols: list):
        """
        Method to normalise symbols. Will be different for each exchange

        :param symbols: List of symbols to normalise
        """
        pass

    @abstractmethod
    def normalise_timestamp(self, ts: dt) -> float:
        """
        Method to normalise timestamps. Will be different for each exchange

        :param ts: Timestamp to normalise
        :type ts: datetime
        """
        pass

    @classmethod
    def get_channel_from_feed(cls, feed) -> str:
        """
        Returns the exchange channel from a standardised feed

        :param feed: Standardised feed
        :type feed: str
        :return: Exchange channel
        :rtype: str
        """
        return cls.ws_channels[feed] if feed in cls.ws_channels else cls.rest_channels[feed]

    @classmethod
    def get_feeds_from_channel(cls, channel: str) -> str:
        """
        Returns the standardised feed from an exchange channel

        :param channel: Exchange channel
        :type channel: str
        :return: Standardised feed
        :rtype: str
        """
        return [k for k, v in cls.ws_channels.items() if v == channel]

    def get_exchange_symbol(self, symbol: str) -> str:
        """
        Returns the exchange symbol from a normalised symbol

        :param symbol: Normalised symbol
        :type symbol: str
        :return: Exchange symbol
        :rtype: str
        """
        return self.symbols[symbol]

    def get_normalised_symbol(self, symbol: str) -> str:
        """
        Returns the normalised symbol from an exchange symbol

        :param symbol: Exchange symbol
        :type symbol: str
        :return: Normalised symbol
        :rtype: str
        """
        return self.inv_symbols[symbol]

    @classmethod
    def _get_field(cls, msg, field):
        if field and isinstance(field, str):
            key = msg.get(field, None)
        else:
            try:
                key = msg[field]
            except (IndexError, KeyError):
                logging.warning(
                    f"Key field {field} not found in message")
                key = None
        return key

    # Override this method if the exchange uses a different method of getting the msg symbol or msg type
    @classmethod
    def get_sym_from_msg(cls, msg):
        return cls._get_field(msg, cls.sym_field)

    @classmethod
    def get_type_from_msg(cls, msg):
        return cls._get_field(msg, cls.type_field)

    # TODO: Use this to simplify the standardisation -- this already retrieves the symbol from the data
    @classmethod
    def get_key(cls, message: dict) -> str:
        """
        Returns the key for the provided message

        :param message: Message to get the key for
        :type message: dict
        :return: Key for the message
        :rtype: str
        """
        s = cls.get_sym_from_msg(message)
        t = cls.get_type_from_msg(message)
        if s and t:
            key = f"{cls.name}_{s}_{t}"
            if isinstance(key, str):
                key = key.encode()
            return key


class DataFeed(DataSource):
    """
    Class to handle the connection to a exchange.

    :param retries: Number of times to retry connecting to the exchange
    :type retries: int, optional
    :param interval: Interval between connection attempts
    :type interval: int, optional
    :param timeout: Timeout for the connection
    :type timeout: int, optional
    :param delay: Delay before starting the connection
    :type delay: int, optional
    """

    def __init__(self, symbols=None, retries=3, interval=30, timeout=120, delay=0, max_syms=10):
        super().__init__(symbols=symbols)
        self.connection_handlers = []
        self.retries = retries
        self.interval = interval
        self.timeout = timeout
        self.delay = delay
        self.kafka_connector = None
        self.num_messages = 0
        self.tot_latency = 0
        self.max_syms = max_syms

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols: list):
        """
        Subscribes to the provided feeds on the provided connection

        :param conn: Connection to subscribe to
        :type conn: AsyncFeed
        :param feeds: Feeds to subscribe to
        :type feeds: list
        :param symbols: Symbols to subscribe to
        :type symbols: list
        """
        pass

    def auth(self, conn: AsyncFeed):
        """
        Authenticates the provided connection

        :param conn: Connection to authenticate
        :type conn: AsyncFeed
        """
        pass

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        """
        First method called when a message is received from the exchange. Currently forwards the message to Kafka to be produced.

        :param message: Message received from the exchange
        :type message: str
        :param conn: Connection the message was received from
        :type conn: AsyncFeed
        :param channel: Channel the message was received on
        :type channel: str
        """
        msg = json.loads(message)
        msg = enrich_raw(msg, timestamp)
        await self.kafka_connector.write(json.dumps(msg))

    def _init_rest(self) -> list:
        """
        Initialises the REST connections

        :return: List of REST connections with details for the connection handler
        :rtype: list
        """
        return []

    def _init_kafka(self, loop: asyncio.AbstractEventLoop):
        """
        Initialises the Kafka connections

        :param loop: Event loop to run the Kafka connection on
        :type loop: asyncio.AbstractEventLoop
        """
        logging.info('%s: Starting Kafka Connector', self.name)
        self.kafka_connector = KafkaConnector(self.__class__)
        self.kafka_connector.register_schemas()
        self.kafka_connector.create_exchange_topics(
            [*self.ws_channels.keys(), *self.rest_channels.keys()])
        self.kafka_connector.start(loop)

    def _pre_start(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Function called before the exchange connection is started. Defaults to nothing, but can be overridden

        :param loop: Event loop to run the connection on
        :type loop: asyncio.AbstractEventLoop
        """
        pass

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Generic WS connection method -- sets up connection handlers for all desired channels and starts the data collection process

        :param loop: Event loop to run the connection on
        :type loop: asyncio.AbstractEventLoop
        """
        self._pre_start and self._pre_start(loop)
        symbols = []
        self._init_kafka(loop)
        rest_connections = self._init_rest()
        for connection in rest_connections:
            self.connection_handlers.append(AsyncConnectionManager(
                connection, None, self.process_message, None, None, self.retries, self.interval, self.timeout, self.delay))
        for (endpoint, channels) in self.ws_endpoints.items():
            for symbol in self.symbols.values():
                if not channels:
                    continue
                url = endpoint.get_url()
                if not url:
                    continue
                symbols.append(symbol)
                if self.max_syms and len(symbols) == self.max_syms:
                    connection = WSConnection(
                        self.name, url, authentication=None, symbols=symbols, **endpoint.options)
                    self.connection_handlers.append(AsyncConnectionManager(
                        connection, self.subscribe, self.process_message, None, channels, self.retries, self.interval, self.timeout, self.delay))
                    symbols = []
        if symbols:
            connection = WSConnection(
                self.name, url, authentication=None, symbols=symbols, **endpoint.options)
            self.connection_handlers.append(AsyncConnectionManager(
                connection, self.subscribe, self.process_message, None, channels, self.retries, self.interval, self.timeout, self.delay))
            symbols = []

        for handler in self.connection_handlers:
            handler.start_connection(loop)

        for chan in [*self.ws_channels.keys(), *self.rest_channels.keys()]:
            logging.info('%s: Starting connection to %s', self.name, chan)

    async def stop(self):
        """
        Stops the connection to the exchange
        """
        logging.info('%s: Shutting down', self.name)
        tasks = []
        if self.kafka_connector:
            tasks.append(self.kafka_connector.stop())
        for handler in self.connection_handlers:
            tasks.append(handler.conn.close())
            handler.running = False
        await asyncio.gather(*tasks)
