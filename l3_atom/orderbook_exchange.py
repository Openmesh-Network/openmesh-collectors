from abc import abstractmethod
from l3_atom.helpers.read_config import get_conf_symbols
from datetime import datetime as dt
from datetime import timedelta, timezone
import asyncio
from yapic import json
import time
from decimal import *
import requests

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection
from l3_atom.tokens import Symbol

from l3_atom.sink_connector.redis_multiprocessed import RedisStreamsConnector
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging


class OrderBookExchange:
    name = NotImplemented
    """
    {
        <WSEndpoint>: [channels to subscribe to],
        <WSEndpoint>: [channels to subscribe to],
        ...
    }
    """
    ws_endpoints: dict = NotImplemented
    rest_endpoints: dict = NotImplemented
    symbols_endpoint = NotImplemented
    ws_channels = NotImplemented
    candle_interval = NotImplemented

    def __init__(self):
        self.symbols = self.filter_symbols(self.normalise_symbols(self.get_symbols(self.name)), get_conf_symbols(self.name))
        self.inv_symbols = {v: k for k, v in self.symbols.items()}

    def get_symbols(self, exchange: str) -> list:
        return requests.get(self.symbols_endpoint).json()

    def filter_symbols(self, sym_list, filters):
        ret = {}
        for norm in filters:
            norm_sym = Symbol(*norm.split('.'))
            ret[norm_sym] = sym_list[norm_sym]
        return ret

    @abstractmethod
    def normalise_symbols(self, symbols: list):
        pass

    @abstractmethod
    def normalise_timestamp(self, ts: dt) -> float:
        pass

    # Helpers that get channel information (e.g.
    # the channel to subscribe to for order book updates,
    # the channel to subscribe to for trade updates, e.t.c.)
    # Does the inverse operation as well
    @classmethod
    def get_feed_from_channel(cls, channel) -> str:
        return cls.ws_channels[channel]

    @classmethod
    def get_channel_from_feed(cls, channel: str) -> str:
        for chan, exch in cls.websocket_channels.items():
            if exch == channel:
                return chan
        return None

    # Gets the exchange symbol from a normalised symbol
    def get_exchange_symbol(self, symbol: str) -> str:
        return self.symbols[symbol]
    
    # Gets the normalised symbol from an exchange symbol
    def get_normalised_symbol(self, symbol: str) -> str:
        return self.inv_symbols[symbol].normalised


class OrderBookExchangeFeed(OrderBookExchange):
    def __init__(self, retries=3, interval=30, timeout=120, delay=0):
        super().__init__()
        self.connection_handlers = []
        self.retries = retries
        self.interval = interval
        self.timeout = timeout
        self.delay = delay
        self.kafka_connector = KafkaConnector(self.name, self.key_field)
        self.num_messages = 0
        self.tot_latency = 0

    # Each exchange has its own way of subscribing to channels and handling incoming messages
    async def subscribe(self, conn: AsyncFeed, channels: list):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, ts: float):
        await self.kafka_connector(message)

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Generic WS connection method -- sets up connection handlers for all desired channels and starts the data collection process
        """
        connections = []
        symbols = []
        max_syms = 10
        for (endpoint, channels) in self.ws_endpoints.items():
            for symbol in self.symbols.values():
                if not channels:
                    continue
                url = endpoint.get_url()
                if not url:
                    continue
                symbols.append(symbol)
                if len(symbols) == max_syms:
                    connections.append((WSConnection(
                        self.name, url, authentication=None, symbols=symbols, **endpoint.options), self.subscribe, self.process_message, None, channels))
                    symbols = []
        if symbols:
            connections.append((WSConnection(
                        self.name, url, authentication=None, symbols=symbols, **endpoint.options), self.subscribe, self.process_message, None, channels))
            symbols = []

        for connection, subscribe, handler, auth, channels in connections:
            self.connection_handlers.append(AsyncConnectionManager(connection, subscribe, handler, auth, channels, self.retries, self.interval, self.timeout, self.delay))
            self.connection_handlers[-1].start_connection(loop)

        logging.info('%s: Starting Kafka Connector', self.name)
        self.kafka_connector.start(loop)

        
