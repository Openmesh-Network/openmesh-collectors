from websocket_manager import WebsocketManager

class CoinbaseWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://ws-feed.pro.coinbase.com'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = \
                {"type": "subscribe",
                "channels": [{"name": "full", "product_ids": [symbol]}]}
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = \
                {"type": "unsubscribe",
                "channels": [{"name": "full", "product_ids": [symbol]}]}
            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager