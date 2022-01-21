from ..websocket_manager import WebsocketManager
from .ws_manager_factory import WsManagerFactory


class KrakenFuturesWsManagerFactory(WsManagerFactory):
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