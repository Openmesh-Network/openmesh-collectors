import asyncio
from multiprocessing import Process, Pipe
from contextlib import asynccontextmanager
from abc import abstractmethod

SHUTDOWN = "END"

class SinkMessageHandler:

    def __init__(self, exchange:str):
        self.exchange = exchange

    def start(self, loop: asyncio.AbstractEventLoop):
        self.started = True
        self.pipe = Pipe(duplex=False)
        self.process = Process(target=SinkMessageHandler.run, args=(self.producer,), daemon=True)
        self.process.start()

    async def stop(self):
        self.pipe[1].send(SHUTDOWN)
        self.process.join()
        self.started = False

    @staticmethod
    def run(producer):
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(producer())
        except KeyboardInterrupt:
            pass

    @abstractmethod
    async def producer(self):
        pass

    async def write(self, data):
        self.pipe[1].send(data)

    @asynccontextmanager
    async def read_from_pipe(self) -> list:
        data = self.pipe[0].recv()
        if data == SHUTDOWN:
            self.started = False
            yield []
        else:
            yield [data]
        
class SinkConnector:
    async def __call__(self, data):
        await self.write(data)