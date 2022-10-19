import asyncio
from multiprocessing import Process, Pipe
from contextlib import asynccontextmanager
from abc import abstractmethod
from typing import Awaitable

SHUTDOWN = "END"


class SinkMessageHandler:
    """
    Class to handle the connection to a back-end sink.

    :param exchange: Name of the exchange
    """

    def __init__(self, exchange):
        self.exchange_ref = exchange
        self.exchange = exchange.name

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Starts the backend process in parallel to the main process. Creates a pipe to communicate.

        :param loop: Event loop to run the backend process on
        :type loop: asyncio.AbstractEventLoop
        """
        self.started = True
        self.pipe = Pipe(duplex=False)
        self.process = Process(target=SinkMessageHandler.run,
                               args=(self.producer,), daemon=True)
        self.process.start()

    async def stop(self):
        """
        Stops the backend process.
        """
        self.pipe[1].send(SHUTDOWN)
        self.process.join()
        self.started = False

    @staticmethod
    def run(producer: Awaitable):
        """
        Starts the backend production method in a new event loop for the backend process.

        :param producer: Method to produce messages to the backend
        :type producer: Awaitable
        """
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(producer())
        except KeyboardInterrupt:
            pass

    @abstractmethod
    async def producer(self):
        """Method to produce messages to the backend. Overriden in child classes."""
        pass

    async def write(self, data):
        """Sends data to the backend process via pipe."""
        self.pipe[1].send(data)

    @asynccontextmanager
    async def read_from_pipe(self) -> list:
        """Asynchronus context manager to read from the pipe."""
        data = self.pipe[0].recv()
        if data == SHUTDOWN:
            self.started = False
            yield []
        else:
            yield [data]
