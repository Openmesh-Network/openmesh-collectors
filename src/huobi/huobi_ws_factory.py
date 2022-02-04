from websocket_manager import WebsocketManager


class HuobiWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Jay"""
        book_url = "wss://api-aws.huobi.pro/feed"
        trades_url = 'wss://api-aws.huobi.pro/ws'

        # Subscribe to channels
        def subscribe_book(ws_manager):
            request = {'sub': f'market.{symbol}.mbp.400', 
                'id': 'id1'
            }
            ws_manager.send_json(request)

        def subscribe_trades(ws_manager):
            request = {'sub': f'market.{symbol}.trade.detail', 
                'id': 'id1'
            }
            ws_manager.send_json(request)


        # Unubscribe from channels
        def unsubscribe_book(ws_manager):
            request = {'unsub': f'market.{symbol}.mbp.400', 
                'id': 'id1'
            }
            ws_manager.send_json(request)

        def unsubscribe_trades(ws_manager):
            request = {'unsub': f'market.{symbol}.trade.detail', 
                'id': 'id1'
            }
            ws_manager.send_json(request)
            
        trades_ws_manager = WebsocketManager(trades_url, subscribe_trades,unsubscribe_trades)
        book_ws_manager = WebsocketManager(book_url, subscribe_book, unsubscribe_book)
        return book_ws_manager, trades_ws_manager

def main():
    ws = HuobiWsManagerFactory().get_ws_manager("btcusdt")
    while True:
        try:
            pass
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()