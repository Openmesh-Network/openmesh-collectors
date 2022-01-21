"""
ws_factories

Uses the factory pattern to create the correct websocket connection to each exchange.
"""
from configparser import ConfigParser
import json
import os
from textwrap import indent
from time import sleep
import time
import requests
from gzip import decompress

from .websocket_manager import WebsocketManager


class FactoryRegistry():
    """Holds the factory classes for each exchange"""

    FACTORIES = [
        "okex",
        "phemex",
        "kraken",
        "kucoin",
        "huobi",
        "ftx"
    ]

    def __init__(self):
        self.factories = {}  # {str: WsManagerFactory}
        self.register()

    def register(self):
        """Initiliases the factory classes for the given exchanges"""
        self.factories["okex"] = OkexWsManagerFactory()
        self.factories["phemex"] = PhemexWsManagerFactory()
        self.factories["kraken"] = KrakenWsManagerFactory()
        self.factories["kucoin"] = KucoinWsManagerFactory()
        self.factories["huobi"] = HuobiWsManagerFactory()
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


class HuobiWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://api-aws.huobi.pro/ws'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'sub': f'market.{symbol}.depth.step0', 
            'id': 'id1'
            }
            ws_manager.send_json(request)

            request['sub'] = f'market.{symbol}.trade.detail'

            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'unsub': f'market.{symbol}.depth.step0', 
            'id': 'id1'
            }
            ws_manager.send_json(request)

            request['unsub'] = f'market.{symbol}.trade.detail'

            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager


class FtxWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://ftx.com/ws/'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'op': 'subscribe', 
            'channel': 'orderbook', 
            'market': symbol
            }
            ws_manager.send_json(request)
            request['channel'] = 'trades'

            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'op': 'unsubscribe', 
                'channel': 'orderbook', 
                'market': symbol
                }
            ws_manager.send_json(request)
            request['channel'] = 'trades'
            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager


class KucoinWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jack"""

        setup_data = requests.post("https://api.kucoin.com/api/v1/bullet-public").json()
        token = setup_data['data']['token']
        endpoint = setup_data['data']["instanceServers"][0]["endpoint"]

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'type': 'subscribe', 
            'topic': f'/market/level2:{symbol}', 
            'id': 1545910660739,
            "privateChannel": False,
            "response": True
            }
            ws_manager.send_json(request)
            request['topic'] = f'/market/match:{symbol}'

            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'type': 'unsubscribe', 
            'topic': f'/market/level2:{symbol}', 
            'id': 1545910660739,
            "privateChannel": False,
            "response": True
            }
            ws_manager.send_json(request)
            request['topic'] = f'/market/match:{symbol}'

            ws_manager.send_json(request)

        url = f"{endpoint}?token={token}&[connectId=1545910660739]"    

        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager


class OkexWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = "wss://ws.okex.com:8443/ws/v5/public"

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {}
            request['op'] = 'subscribe'
            request['args'] = [{"channel": "books", "instId": symbol}]
            ws_manager.send_json(request)
            request['args'] = [{"channel": "trades", "instId": symbol}]
            ws_manager.send_json(request)


        # Unubscribe from channels
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
        """Rayman"""
        url = "wss://phemex.com/ws"

        # Subscribe to channels
        def subscribe(ws_manager):

            request = {
                "id": 1234,  # random id
                "method": "orderbook.subscribe",
                "params": [symbol]
            }
            ws_manager.send_json(request)

            request['method'] = "trade.subscribe"
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {
                "id": 1234,
                "method": "orderbook.unsubscribe",
                "params": [symbol]
            }
            ws_manager.send_json(request)

            request['method'] = "trades.unsubscribe"
            ws_manager.send_json(request)

        ws_manager = WebsocketManager(url, subscribe, unsubscribe)
        return ws_manager


if __name__ == "__main__":
    ws_manager = FactoryRegistry().get_ws_manager("phemex")
    while True:
        try:
            print(json.dumps(ws_manager.get_msg(), indent=4))
        except KeyboardInterrupt:
            break
