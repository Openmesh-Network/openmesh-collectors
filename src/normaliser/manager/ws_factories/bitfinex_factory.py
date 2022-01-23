from ..websocket_manager import WebsocketManager
from .ws_manager_factory import WsManagerFactory


class BitfinexWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Rayman"""
        url = "wss://api-pub.bitfinex.com/"

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {
                'event': 'subscribe', 
                'channel': 'book', 
                'symbol': symbol,
                'len': "250",
            }
            ws_manager.send_json(request)

            request['channel'] = 'trades'
            request['chanId'] = 1
            ws_manager.send_json(request)

        # Unubscribe from channels
        # handled in the normalisation function as Bitfinex sends a channel id back for unsubscribing
        def unsubscribe(ws_manager):
            request = {
                'op': 'unsubscribe', 
                'chanId': None
            }
            ws_manager.send_json(request)

            request['chanId'] = None
            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager