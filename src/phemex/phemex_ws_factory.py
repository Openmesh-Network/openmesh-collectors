from websocket_manager import WebsocketManager


class PhemexWsManagerFactory():
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