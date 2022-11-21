from random import random
import time
import asyncio
from typing import Awaitable, Iterator, Union
import websockets
import aiohttp
from contextlib import asynccontextmanager
import logging
import sys

from websockets import ConnectionClosed
from websockets.exceptions import InvalidStatusCode

from l3_atom.exceptions import TooManyRetries

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%d/%b/%Y %H:%M:%S",
    stream=sys.stdout)


class Feed:
    """
    Parent class for all feeds.
    """
    async def send_data(self, data):
        """
        Generic function to send data over a connection.

        :param data: Data to send.
        """
        raise NotImplementedError

    async def read_data(self):
        """
        Generic function to read data from a connection.
        """
        raise NotImplementedError


class AsyncFeed(Feed):
    """
    Extension of Feed that uses asyncio and coroutines for most functionality

    :param id: unique identifier for the feed
    :type id: str
    :param authentication: authentication function to be called before attempting to connect
    :type authentication: Callable, optional
    :param symbols: list of symbols to subscribe to
    :type symbols: list, optional
    """

    def __init__(self, id: str, authentication: Awaitable = None, symbols: list = None):
        self.id = id
        self.received_messages: int = 0
        self.sent_messages: int = 0
        self.start_time = None
        self.symbols = symbols
        self.conn: Union[websockets.WebSocketClientProtocol,
                         aiohttp.ClientSession] = None
        self.authentication = authentication
        self.last_received_time = None

    def get_time_us(self):
        return time.time_ns() // 1000

    @asynccontextmanager
    async def connect(self):
        """
        Asynchronous context manager for connecting to a feed. Allows for elegant opening and closing
        """
        await self._open()
        try:
            yield self
        finally:
            await self.close()

    async def _open(self):
        """
        Function to open a connection to a feed. Should be overridden by subclasses.
        """
        raise NotImplementedError

    @property
    def is_open(self) -> bool:
        """
        Determines whether the connection is open.

        :return: True if the connection is open, False otherwise
        """
        return self.conn and not self.conn.closed

    async def close(self):
        """
        Closes the connection to the feed.
        """
        if self.is_open:
            conn = self.conn
            self.conn = None
            await conn.close()
            logging.info('%s: closed connection after %d messages sent, %d messages received (%.2fs)', self.id, self.sent_messages, self.received_messages, (self.get_time_us() - self.start_time
                                                                                                                                                             ) / 1e6)


class HTTPConnection(AsyncFeed):
    """
    Async connection to a REST endpoint, polls periodically

    :param id: unique identifier for the feed
    :type id: str
    :param addr: address of the REST endpoint
    :type addr: str
    :param poll_frequency: seconds between each poll
    :type poll_frequency: int, optional
    :param retry: seconds to wait before retrying a failed connection
    :type retry: int, optional
    :param rate_limit_retry: seconds to wait before retrying after a rate limit error
    :type rate_limit_retry: int, optional
    :param authentication: authentication function to be called before attempting to connect
    :type authentication: Callable, optional
    :param symbols: list of symbols to poll for
    :type symbols: list, optional
    """

    def __init__(self, id: str, addr: str, poll_frequency: int = 60, retry: int = 5, rate_limit_retry: int = 60, authentication: Awaitable = None, symbols: list = None):
        super().__init__(f'http:{id}',
                         authentication=authentication, symbols=symbols)
        self.addr = addr
        self.poll_frequency = poll_frequency
        self.retry = retry
        self.rate_limit_retry = rate_limit_retry

    async def _open(self):
        """
        Opens an asynchronous HTTP connection to the REST endpoint.
        """
        if self.is_open:
            return
        self.conn = aiohttp.ClientSession()
        logging.info('%s: opened connection %r', self.id,
                     self.conn.__class__.__name__)
        self.sent_messages = 0
        self.received_messages = 0
        self.start_time = self.get_time_us()

    async def _get_data(self, url):
        """
        Retrieves data from a given URL via a GET request.

        :param url: URL to retrieve data from
        :type url: str
        :return: data retrieved from the URL
        """
        if not self.is_open:
            await self._open()
        while True:
            try:
                async with self.conn.get(url) as resp:
                    self.sent_messages += 1
                    self.received_messages += 1
                    self.last_received_time = self.get_time_us()
                    if resp.status != 200:
                        logging.error('%s: received status code %d',
                                      self.id, resp.status)
                        if resp.status == 429:
                            logging.error(
                                '%s: rate limit exceeded, retrying in %d seconds', self.id, self.rate_limit_retry)
                            await asyncio.sleep(self.rate_limit_retry)
                    else:
                        return await resp.text()
            except (ConnectionClosed) as e:
                logging.error('%s: %s', self.id, e)
                await self.close()
                await asyncio.sleep(self.retry)
                await self._open()

    async def read_data(self) -> Iterator[str]:
        """
        Periodically polls the REST endpoint for data.

        :return: data retrieved from the REST endpoint
        """
        while True:
            yield await self._get_data(self.addr)
            await asyncio.sleep(self.poll_frequency)


class WSConnection(AsyncFeed):
    """
    Websocket connection to a feed.

    :param id: unique identifier for the feed
    :type id: str
    :param url: address of the websocket endpoint
    :type url: str
    :param authentication: authentication function to be called before attempting to connect
    :type authentication: Callable, optional
    :param symbols: list of symbols to subscribe to
    :type symbols: list, optional
    """

    def __init__(self, id: str, url: str, authentication: Awaitable = None, symbols: list = None, **kwargs):
        super().__init__(f'ws:{id}',
                         authentication=authentication, symbols=symbols)
        self.url = url
        self.options = kwargs

    async def _open(self):
        """
        Opens the connection to the websocket endpoint.
        """
        if self.is_open:
            return
        if self.authentication:
            self.address, self.options = await self.authentication(self.address, self.options)
        self.conn = await websockets.connect(self.url, **self.options)
        logging.info('%s: opened connection %r', self.id,
                     self.conn.__class__.__name__)
        self.start_time = self.get_time_us()
        self.sent_messages = 0
        self.received_messages = 0

    async def send_data(self, data):
        """
        Sends data over the websocket connection.

        :param data: data to send
        """
        if not self.is_open:
            await self._open()
        self.sent_messages += 1
        await self.conn.send(data)

    async def read_data(self):
        """
        Reads data from the websocket connection.

        :return: data received from the websocket connection
        """
        if not self.is_open:
            await self._open()
        async for data in self.conn:
            self.received_messages += 1
            self.last_received_time = self.get_time_us()
            yield data


class AsyncConnectionManager:
    """
    Manages an asynchronous connection to a feed -- handling errors, rate limits, e.t.c.

    :param conn: Asynchronous connection to some feed
    :type conn: AsyncFeed
    :param subscribe: function to be called to subscribe to the right feed
    :type subscribe: Awaitable
    :param unsubscribe: function to be called to unsubscribe from the feed
    :type unsubscribe: Awaitable
    :param auth: authentication function to be called before attempting to connect
    :type auth: Awaitable
    :param channels: list of feeds to subscribe to
    :type channels: list
    :param retries: number of times to retry a failed connection
    :type retries: int
    :param interval: seconds to wait before checking a dead connection
    :type interval: int, optional
    :param timeout: seconds after which a connection is considered dead
    :type timeout: int, optional
    :param delay: seconds to wait before starting the initial connection
    :type delay: int, optional
    """

    def __init__(self, conn: AsyncFeed, subscribe: Awaitable, callback: Awaitable, auth: Awaitable, channels, retries: int = 5, interval: int = 30, timeout: int = 120, delay: int = 0):
        self.conn = conn
        self.subscribe = subscribe
        self.callback = callback
        self.auth = auth
        self.channels = channels
        self.max_retries = retries
        self.interval = interval
        self.timeout = timeout * 1e6
        self.delay = delay
        self.running = True

    def start_connection(self, async_loop: asyncio.AbstractEventLoop):
        """
        Starts the connection to the feed.

        :param async_loop: event loop to run the connection in
        :type async_loop: asyncio.AbstractEventLoop
        """
        async_loop.create_task(self._setup_connection())

    async def _monitor(self):
        """
        Periodically checks the connection to the feed to determine if it is still alive.
        """
        while self.conn.is_open:
            if self.conn.last_received_time:
                if self.conn.get_time_us() - self.conn.last_received_time > self.timeout:
                    logging.warning(
                        "%s: timeout window received 0 messages, restarting", self.conn.id)
                    await self.conn.close()
                    break
            await asyncio.sleep(self.interval)

    async def _setup_connection(self):
        """
        Initializes the connection to the feed. Handles disconnects, errors, rate limits, e.t.c. and links to callback function.
        """
        await asyncio.sleep(self.delay)
        delay = limited = 1
        retries = 0
        while self.running:
            try:
                await self.conn._open()
                self.auth and await self.auth()
                self.subscribe and await self.subscribe(self.conn, self.channels, self.conn.symbols)
                delay = limited = 1
                retries = 0
                loop = asyncio.get_event_loop()
                loop.create_task(self._monitor())
                logging.info('%s: connection established', self.conn.id)
                async for data in self.conn.read_data():
                    if not self.running:
                        logging.info(
                            '%s: Terminating the connection callback as manager is not running', self.conn.id)
                        await self.conn.close()
                        return
                    await self.callback(data, self.conn, self.conn.last_received_time)
            except InvalidStatusCode as e:
                code = e.status_code
                if code == 429:
                    wait = int(e.headers.get('Retry-After', -1))
                    if wait == -1:
                        wait = limited * random.uniform(0.8, 1.2) * 60
                        limited += 1
                    logging.warning(
                        '%s: rate limit exceeded, retrying in %d seconds', self.conn.id, wait)
                    await asyncio.sleep(wait)
                elif code == 401:
                    logging.warning(
                        '%s: authentication failed, retrying in %d seconds', self.conn.id, delay)
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    logging.warning(
                        '%s: invalid status code %d, retrying in %d seconds', self.conn.id, code, delay)
                    await asyncio.sleep(delay)
                    delay *= 2
                    retries += 1
            except Exception as e:
                logging.warning(
                    '%s: Encountered Exception %s, retrying in %d seconds', self.conn.id, e, delay)
                await asyncio.sleep(delay)
                delay *= 2
                retries += 1

        logging.info(
                '%s: connection closed after %d retries', self.conn.id, retries)


class WSEndpoint:
    """
    Class to handle the connection to a websocket endpoint.

    :param main_url: main url of the websocket endpoint
    :type main_url: str
    :param sandbox_url: sandbox url, if the exchange supports it
    :type sandbox_url: str, optional
    :param authentication: whether or not the endpoint requires authentication
    :type authentication: bool, optional
    :param options: options to be passed to the websocket connection
    :type options: dict, optional
    :param limit: limit on number of open subscripitons on a single connection
    :type limit: int, optional
    """

    def __init__(self, main_url: str, sandbox_url: str = None, authentication: bool = None, options: dict = None, limit: int = None):
        self.main_url = main_url
        self.sandbox_url = sandbox_url
        self.authentication = authentication
        self.limit = limit
        default = {'max_size': 2**23, 'max_queue': None,
                   'ping_interval': 10, 'ping_timeout': None}
        if options:
            self.options = options
            self.options.update(default)
        else:
            self.options = default

    def get_url(self):
        """
        Returns the url of the websocket endpoint. Can be overloaded if the url needs to be calculated dynamically, e.g. with a token.

        :return: url of the websocket endpoint
        """
        return self.main_url
