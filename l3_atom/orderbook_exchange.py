from abc import abstractmethod
from l3_atom.helpers.read_config import get_symbols
from datetime import datetime as dt
import asyncio
import json

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection

from l3_atom.sink_connector.redis_multiprocessed import RedisStreamsConnector

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
    ws_channels = NotImplemented
    candle_interval = NotImplemented

    def __init__(self):
        self.symbols = self.normalize_symbols(get_symbols(self.name))

    @abstractmethod
    def normalize_symbols(self, symbols: list):
        pass

    @abstractmethod
    def normalize_timestamp(self, ts: dt) -> float:
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
        for sym, exch in self.symbols.items():
            if exch == symbol:
                return sym
        return None


class OrderBookExchangeFeed(OrderBookExchange):
    def __init__(self, retries=3, interval=30, timeout=120, delay=0):
        super().__init__()
        self.connection_handlers = []
        self.retries = retries
        self.interval = interval
        self.timeout = timeout
        self.delay = delay
        self.redis_connector = RedisStreamsConnector(self.name)

    # Each exchange has its own way of subscribing to channels and handling incoming messages
    async def subscribe(self, conn: AsyncFeed, channels: list):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, ts: float):
        # print(json.dumps(json.loads(message), indent=4))
        await self.redis_connector(message)

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Generic WS connection method -- sets up connection handlers for all desired channels and starts the data collection process
        """
        connections = []
        for (endpoint, channels) in self.ws_endpoints.items():
            if not channels:
                continue
            url = endpoint.get_url()
            if not url:
                continue
            connections.append((WSConnection(
                self.name, url, authentication=None, **endpoint.options), self.subscribe, self.process_message, None, channels))

        for connection, subscribe, handler, auth, channels in connections:
            self.connection_handlers.append(AsyncConnectionManager(connection, subscribe, handler, auth, channels, self.retries, self.interval, self.timeout, self.delay))
            self.connection_handlers[-1].start_connection(loop)

        logging.info('%s: Starting Redis Connector', self.name)
        self.redis_connector.start(loop)

        
