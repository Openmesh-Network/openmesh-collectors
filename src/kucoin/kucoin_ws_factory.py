import requests

from websocket_manager import WebsocketManager


class KucoinWsManagerFactory():
    def get_ws_manager(self, symbol: str):
        """Jay"""

        setup_data = requests.post("https://api.kucoin.com/api/v1/bullet-public").json()
        token = setup_data['data']['token']
        endpoint = setup_data['data']["instanceServers"][0]["endpoint"]

        # Subscribe to channels
        def subscribe(ws_manager):
            request = {'type': 'subscribe', 
            'topic': f'/market/level2:{symbol}', 
            'id': 1545910660739,
            "privateChannel": False,
            "response": True
            }
            ws_manager.send_json(request)
            request['topic'] = f'/market/match:{symbol}'

            ws_manager.send_json(request)

        # Unubscribe from channels
        def unsubscribe(ws_manager):
            request = {'type': 'unsubscribe', 
            'topic': f'/market/level2:{symbol}', 
            'id': 1545910660739,
            "privateChannel": False,
            "response": True
            }
            ws_manager.send_json(request)
            request['topic'] = f'/market/match:{symbol}'

            ws_manager.send_json(request)

        url = f"{endpoint}?token={token}&[connectId=1545910660739]"    

        ws_manager = WebsocketManager(url,subscribe,unsubscribe)
        return ws_manager
