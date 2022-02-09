from websocket_manager import WebsocketManager


class OkexWsManagerFactory():
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


def main():
    ws = OkexWsManagerFactory().get_ws_manager("BTC-USDT")
    while True:
        try:
            1+1
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()