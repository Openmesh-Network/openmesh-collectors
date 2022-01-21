from ..websocket_manager import WebsocketManager

class WsManagerFactory():
    def get_ws_manager(self, symbol: str) -> WebsocketManager:
        """
        Returns the websocket manager created by the factory.

        :param symbol: symbol of the ticker to subscribe to
        """
        raise NotImplementedError()