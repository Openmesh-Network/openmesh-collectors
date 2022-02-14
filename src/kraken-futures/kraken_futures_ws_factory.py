from websocket_manager import WebsocketManager


class KrakenFuturesWsManagerFactory():
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

def main():
    ws = KrakenFuturesWsManagerFactory().get_ws_manager("PI_XBTUSD")
    #print("websocket created")
    while True:
        try:
            1+1
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()