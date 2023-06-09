from l3_atom.data_source import DataFeed
from l3_atom.feed import WSEndpoint, AsyncFeed
from l3_atom.helpers.enrich_data import enrich_raw
from l3_atom.exceptions import APIKeyRequired
from yapic import json
import dotenv
import os
from json import dumps

class OpenSea(DataFeed):
    name = "opensea"
    type_field = 'event'

    ws_endpoints = {
        WSEndpoint("wss://stream.openseabeta.com/socket/websocket"): ["nft_trades"]
    }

    ws_channels = {
        "nft_trades": "item_sold"
    }

    def __init__(self, *args, api_key=None, **kwargs):
        super().__init__(*args, **kwargs)
        if api_key is None:
            dotenv.load_dotenv('keys/.env')
            api_key = os.environ.get('L3A_OPENSEA_API_KEY', None)
        if api_key is None:
            raise APIKeyRequired("OpenSea API key required")
        self.api_key = api_key
        self.symbols = {'all': 'all'}

        # OpenSea requires that the auth token (generated manually on the site) be passed as a query param, so WS URL must be dynamically constructed
        self.ws_endpoints = {
            WSEndpoint(f"wss://stream.openseabeta.com/socket/websocket?token={self.api_key}"): ["nft_trades"]
        }

    @classmethod
    def get_sym_from_msg(cls, msg):
        return msg['payload']['payload']['collection']['slug']

    def normalise_symbols(self, sym_list: list) -> dict:
        return None
    
    def filter_symbols(self, sym_list: dict, filters: dict) -> dict:
        return None

    async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
        # OpenSea API requires you to subscribe to all feeds
        msg = {
            "topic": "collection:*",
            "event": "phx_join",
            "payload": {},
            "ref": 0
        }
        await conn.send_data(json.dumps(msg))

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        """
        First method called when a message is received from the exchange. Currently forwards the message to Kafka to be produced.

        :param message: Message received from the exchange
        :type message: str
        :param conn: Connection the message was received from
        :type conn: AsyncFeed
        :param channel: Channel the message was received on
        :type channel: str
        """
        msg = json.loads(message)
        if msg['event'] != 'item_sold':
            return
        msg = enrich_raw(msg, timestamp)
        await self.kafka_connector.write(json.dumps(msg))
        