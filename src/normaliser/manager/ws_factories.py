"""
ws_factories

Uses the factory pattern to create the correct websocket connection to each exchange.
"""
from configparser import ConfigParser
import json
import os

from .websocket_manager import WebsocketManager


class FactoryRegistry():
    FACTORIES = [
        "okex",
        "phemex",
        "kraken",
        "kucoin",
        "bitfinex",
        "ftx"
    ]

    def __init__(self):
        self.factories = {}  # {str: WsManagerFactory}
        self.register()

    def register(self):
        self.factories["okex"] = OkexWsManagerFactory()
        self.factories["phemex"] = PhemexWsManagerFactory()
        self.factories["kraken"] = KrakenWsManagerFactory()
        self.factories["kucoin"] = KucoinWsManagerFactory()
        self.factories["deribit"] = DeribitWsManagerFactory()
        self.factories["ftx"] = FtxWsManagerFactory()

    def get_ws_manager(self, exchange_id: str, symbol: str):
        if not exchange_id in self.factories.keys():
            raise KeyError(
                f"exchange id {exchange_id} not registered as a factory")
        return self.factories[exchange_id].get_ws_manager(symbol)


class WsManagerFactory():
    def get_ws_manager(self, symbol: str) -> WebsocketManager:
        """
        Returns the websocket manager created by the factory.

        You can add any other private method to your implementation of the class.
        Make sure that get_ws_manager is the ONLY public method in your implementation.
        (Prefix private methods with an underscore).

        :param symbol: symbol of the ticker to subscribe to
        """
        raise NotImplementedError()


class KrakenWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Rayman"""
        url = 'wss://futures.kraken.com/ws/v1'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {
                "event": "subscribe",
                "feed": "book",
                "product_ids": [symbol]
            }
            ws_manager.send_json(request)

            request["feed"] = "trade"
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {
                "event": "unsubscribe",
                "feed": "book",
                "product_ids": [symbol]
            }
            ws_manager.send_json(request)

            request["feed"] = "trade"
            ws_manager.send_json(request)

        ws_manager = WebsocketManager(url, subscribe, unsubscribe)
        return ws_manager


class DeribitWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Vivek"""
        url = None
        ws_manager = WebsocketManager(url)

        # Subscribe to channels

        return ws_manager


class FtxWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Taras"""
        url = None
        ws_manager = WebsocketManager(url)

        # Subscribe to channels

        return ws_manager


class KucoinWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jack"""
        url = None
        ws_manager = WebsocketManager(url)

        # Subscribe to channels

        return ws_manager


class OkexWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = "wss://ws.okex.com:8443/ws/v5/public"

        def subscribe(ws_manager):
            request = {}
            request['op'] = 'subscribe'
            request['args'] = [{"channel": "books", "instId": symbol}]
            ws_manager.send_json(request)
            request['args'] = [{"channel": "trades", "instId": symbol}]
            ws_manager.send_json(request)

        def unsubscribe(ws_manager):
            request = {}
            request['op'] = 'unsubscribe'
            request['args'] = [{"channel": "books", "instId": symbol}]
            ws_manager.send_json(request)
            request['args'] = [{"channel": "trades", "instId": symbol}]
            ws_manager.send_json(request)

        ws_manager = WebsocketManager(url, subscribe, unsubscribe)
        return ws_manager


class PhemexWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Will"""
        url = "wss://phemex.com/ws"
        ws_manager = WebsocketManager(url)


if __name__ == "__main__":
    ws_manager = FactoryRegistry().get_ws_manager("kraken")
    while True:
        try:
            print(json.dumps(ws_manager.get_msg()))
        except KeyboardInterrupt:
            break
