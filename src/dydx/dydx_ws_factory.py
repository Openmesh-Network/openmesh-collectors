from websocket_manager import WebsocketManager

class DydxWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://api.dydx.exchange/v3/ws'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'type': 'subscribe', 
            'channel': 'v3_orderbook', 
            'id': symbol,
            'includeOffsets': True
            }
            ws_manager.send_json(request)
            request['channel'] = 'v3_trades'
            del request['includeOffsets']
            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'type': 'unsubscribe', 
            'channel': 'v3_orderbook', 
            'id': symbol,
            'includeOffsets': True
            }
            ws_manager.send_json(request)
            request['channel'] = 'v3_trades'
            del request['includeOffsets']
            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager

def main():
    ws = DydxWsManagerFactory().get_ws_manager("BTC-USD")
    while True:
        try:
            1+1
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()