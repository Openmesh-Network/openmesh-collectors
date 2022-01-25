from websocket_manager import WebsocketManager


class KrakenWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Rayman"""
        url = 'wss://ws.kraken.com'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {
                "event": "subscribe",
                "pair": [symbol],
                "subscription": {
                    "name": "book",
                    "depth": 1000
                }
            }
            ws_manager.send_json(request)

            del request["subscription"]["depth"]
            request["subscription"]["name"] = "trade"
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {
                "event": "unsubscribe",
                "pair": [symbol],
                "subscription": {
                    "name": "book",
                    "depth": 1000
                }
            }
            ws_manager.send_json(request)

            del request["subscription"]["depth"]
            request["subscription"]["name"] = "trade"
            ws_manager.send_json(request)

        ws_manager = WebsocketManager(url, subscribe, unsubscribe)
        return ws_manager