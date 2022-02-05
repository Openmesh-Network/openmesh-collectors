import asyncio
from .bybit_ws import BybitWebsocket
from .ftx_ws import FtxWebsocket
from .kraken_ws import KrakenWebsocket
from .async_wrapper import AsyncConsumer
from .okex_consumer import OkexConsumer 


class ConsumerRegistry():
    def __init__(self):
        # I'll wrap the KafkaConsumer in a threading class later
        self.registry = dict() # {str: KakfaConsumer}
        self._register()
    
    async def get(self, topic_id):
        ws = self.registry[topic_id]
        await ws.start_ws()
        return ws

    def _register(self):
        self.registry["bybit"] = BybitWebsocket()
        self.registry["ftx"] = FtxWebsocket()
        self.registry["kraken"] = KrakenWebsocket()
        # self.registry["okex"] = AsyncConsumer(OkexConsumer("BTCUSDT"))