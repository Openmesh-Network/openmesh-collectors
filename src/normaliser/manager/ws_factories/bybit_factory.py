from ..websocket_manager import WebsocketManager
from .ws_manager_factory import WsManagerFactory


class BybitWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Rayman"""
        url = 'wss://stream.bybit.com/realtime'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {
                "op": "subscribe",
                "args": ["orderBook_200.100ms." + symbol, "trade." + symbol]
            }
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {
                "op": "subscribe",
                "args": ["orderBook_200.100ms." + symbol, "trade." + symbol]
            }

            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager