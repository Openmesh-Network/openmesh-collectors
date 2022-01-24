from websocket_manager import WebsocketManager

class FtxWsManagerFactory():
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