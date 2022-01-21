"""
ws_registry

Factory registry for websocket manager factories.
"""
import json

from ..websocket_manager import WebsocketManager
from .okex_factory import OkexWsManagerFactory
from .kraken_factory import KrakenWsManagerFactory
from .kraken_futures_factory import KrakenFuturesWsManagerFactory
from .phemex_factory import PhemexWsManagerFactory
from .kucoin_factory import KucoinWsManagerFactory
from .huobi_factory import HuobiWsManagerFactory
from .ftx_factory import FtxWsManagerFactory


class FactoryRegistry():

    def __init__(self):
        """Factory registry mapping the exchange id to the respective websocket manager factories."""
        self.factories = {}  # {str: WsManagerFactory}
        self.register()

    def register(self):
        """Initiliases the factory classes for the given exchanges"""
        self.factories["okex"] = OkexWsManagerFactory()
        self.factories["phemex"] = PhemexWsManagerFactory()
        self.factories["kraken"] = KrakenWsManagerFactory()
        self.factories["kraken-futures"] = KrakenFuturesWsManagerFactory()
        self.factories["kucoin"] = KucoinWsManagerFactory()
        self.factories["huobi"] = HuobiWsManagerFactory()
        self.factories["ftx"] = FtxWsManagerFactory()

    def get_ws_manager(self, exchange_id: str, symbol: str):
        if not exchange_id in self.factories.keys():
            raise KeyError(
                f"exchange id {exchange_id} not registered as a factory")
        return self.factories[exchange_id].get_ws_manager(symbol)


if __name__ == "__main__":
    ws_manager = FactoryRegistry().get_ws_manager("phemex")
    while True:
        try:
            print(json.dumps(ws_manager.get_msg(), indent=4))
        except KeyboardInterrupt:
            break
