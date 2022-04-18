from websocket_manager import WebsocketManager

class DeribitWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://www.deribit.com/ws/api/v2'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = \
                {"jsonrpc": "2.0",
                "method": "public/subscribe",
                "id": 42,
                "params": {
                    "channels": [f"book.{symbol}.100ms", f"trades.{symbol}.100ms"]}
                }
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = \
                {"jsonrpc": "2.0",
                "method": "public/unsubscribe",
                "id": 42,
                "params": {
                    "channels": [f"book.{symbol}.100ms", f"trades.{symbol}.100ms"]}
                }
            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager


def main():
    ws = DeribitWsManagerFactory().get_ws_manager("BTC-PERPETUAL")
    while True:
        try:
            pass
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()