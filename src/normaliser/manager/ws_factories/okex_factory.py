from ..websocket_manager import WebsocketManager
from .ws_manager_factory import WsManagerFactory


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