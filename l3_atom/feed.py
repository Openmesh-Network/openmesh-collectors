import time
import asyncio
from typing import Awaitable, Union, AsyncIterable
import websockets
import aiohttp
from contextlib import asynccontextmanager
import logging
import sys
import random

from websockets import ConnectionClosed
from websockets.exceptions import InvalidStatusCode

from dataclasses import dataclass

from l3_atom.exceptions import ConnectionNotOpen, TooManyRetries

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)

class Feed:
    """
    Parent class for all feeds.
    """
    async def send_data(self):
        raise NotImplementedError

    async def read_data(self):
        raise NotImplementedError

class AsyncFeed(Feed):
    """
    Extension of Feed that uses asyncio and coroutines for most functionality
    """

    def __init__(self, id, authentication=None):
        """
        id: unique identifier for this feed
        authentication: authentication function to be called before attempting to connect
        """
        self.id = id
        self.received_messages: int = 0
        self.sent_messages: int = 0
        self.start_time = None
        self.conn: Union[websockets.WebSocketClientProtocol, aiohttp.ClientSession] = None
        self.authentication = authentication
        self.last_received_time = None

    
    @asynccontextmanager
    async def connect(self):
        await self._open()
        try:
            yield self
        finally:
            await self.close()

    async def _open(self):
        raise NotImplementedError

    @property
    def is_open(self) -> bool:
        raise NotImplementedError

    async def close(self):
        if self.is_open:
            conn = self.conn
            self.conn = None
            await conn.close()
            logging.info('%s: closed connection %r after %d messages sent, %d messages, received (%ds)', self.id, conn.__class__.__name__, self.sent_messages, self.received_messages, time.time() - self.start_time)

class WSConnection(AsyncFeed):
    """
    Websocket connection to a feed.
    """
    def __init__(self, id, url, authentication=None, **kwargs):
        super().__init__(f'ws:{id}', authentication=authentication)
        self.url = url
        self.auth_kwargs = kwargs

    async def _open(self):
        if self.is_open:
            return
        if self.authentication:
            self.address, self.ws_kwargs = await self.authentication(self.address, self.ws_kwargs)
        self.conn = await websockets.connect(self.url)
        logging.info('%s: opened connection %r', self.id, self.conn.__class__.__name__)
        self.start_time = time.time()
        self.sent_messages = 0
        self.received_messages = 0

    @property
    def is_open(self) -> bool:
        return self.conn is not None and not self.conn.closed

    async def send_data(self, data):
        if not self.is_open:
            raise ConnectionNotOpen
        self.sent_messages += 1
        await self.conn.send(data)

    async def read_data(self):
        if not self.is_open:
            raise ConnectionNotOpen
        async for data in self.conn:
            self.received_messages += 1
            self.last_received_time = time.time()
            yield data

class AsyncConnectionManager:
    """
    Manages an asynchronous connection to a feed -- handling errors, rate limits, e.t.c.
    """

    def __init__(self, conn: AsyncFeed, subscribe: Awaitable, callback: Awaitable, auth: Awaitable, retries: int, interval=30, timemout=120, delay=0):
        self.conn = conn
        self.subscribe = subscribe
        self.callback = callback
        self.auth = auth
        self.retries = retries
        self.interval = interval
        self.timeout = timemout

    def start_connection(self, async_loop):
        async_loop.create_task(self._setup_connection())

    async def _monitor(self):
        while self.conn.is_open and self.running:
            if self.conn.last_message:
                if time.time() - self.conn.last_message > self.timeout:
                    logging.warning("%s: timeout window received 0 messages, restarting", self.conn.id)
                    await self.conn.close()
                    break
            await asyncio.sleep(self.timeout_interval)

    async def _setup_connection(self):
        await asyncio.sleep(self.delay)
        retries = 0
        delay = 1
        limited = 1
        while (retries <= self.retries or self.retries == -1) and self.running:
            try:
                async with self.conn.connect() as connection:
                    await self.auth(connection)
                    await self.subscribe(connection)
                    retries = 0
                    limited = 0
                    delay = 1
                    if self.timeout != -1:
                        loop = asyncio.get_running_loop()
                        loop.create_task(self._watcher())
                    await self._callback(connection, self.callback)
            except (ConnectionClosed, ConnectionAbortedError, ConnectionResetError) as e:
                logging.warning("%s: connection issue - %s. reconnecting in %.1f seconds...", self.conn.id, str(e), delay, exc_info=True)
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2
            except InvalidStatusCode as e:
                if e.status_code == 429:
                    rand = random.uniform(1.0, 3.0)
                    logging.warning("%s: Rate Limited - waiting %d seconds to reconnect", self.conn.id, (limited * 60 * rand))
                    await asyncio.sleep(limited * 60 * rand)
                    limited += 1
                else:
                    logging.warning("%s: encountered connection issue %s. reconnecting in %.1f seconds...", self.conn.id, str(e), delay, exc_info=True)
                    await asyncio.sleep(delay)
                    retries += 1
                    delay *= 2
            except Exception as e:
                logging.error("%s: encountered an exception, reconnecting in %.1f seconds", self.conn.id, delay, exc_info=True)
                await asyncio.sleep(delay)
                retries += 1
                delay *= 2

        if not self.running:
            logging.info('%s: terminate the connection callback because not running', self.conn.id)
        else:
            logging.error('%s: failed to reconnect after %d retries - exiting', self.conn.id, retries)
            raise TooManyRetries()

    async def _callback(self, connection, callback):
        async for data in connection:
            if not self.running:
                logging.info('%s: terminating the connection callback as manager is not running', self.conn.id)
                await connection.close()
                return
            await callback(data, connection, self.conn.last_received_time)

@dataclass
class WebsocketEndpoint:
    address: str
    sandbox: str = None
    instrument_filter: str = None
    channel_filter: str = None
    limit: int = None
    options: dict = None
    authentication: bool = None

    def __post_init__(self):
        defaults = {'ping_interval': 10, 'ping_timeout': None, 'max_size': 2**23, 'max_queue': None}
        if self.options:
            defaults.update(self.options)
        self.options = defaults

    def get_address(self, sandbox=False):
        return self.sandbox if sandbox and self.sandbox else self.address
