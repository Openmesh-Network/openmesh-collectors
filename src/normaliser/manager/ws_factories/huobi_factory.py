from ..websocket_manager import WebsocketManager
from .ws_manager_factory import WsManagerFactory


class HuobiWsManagerFactory(WsManagerFactory):
    def get_ws_manager(self, symbol: str):
        """Jay"""
        url = 'wss://api-aws.huobi.pro/ws'

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'sub': f'market.{symbol}.depth.step0', 
            'id': 'id1'
            }
            ws_manager.send_json(request)

            request['sub'] = f'market.{symbol}.trade.detail'

            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'unsub': f'market.{symbol}.depth.step0', 
            'id': 'id1'
            }
            ws_manager.send_json(request)

            request['unsub'] = f'market.{symbol}.trade.detail'

            ws_manager.send_json(request)

            
        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager