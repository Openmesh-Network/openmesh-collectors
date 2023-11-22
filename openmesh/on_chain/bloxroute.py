from openmesh.on_chain.ethereum import EthereumTransaction
from openmesh.exceptions import APIKeyRequired
from openmesh.chain import ChainFeed
import dotenv
import os

class Bloxroute(ChainFeed):

    def __init__(self, *args, api_key=None, **kwargs):
        super().__init__(*args, **kwargs)
        if api_key is None:
            dotenv.load_dotenv('keys/.env')
            api_key = os.environ.get('L3A_BLOXROUTE_API_KEY', None)
        
        self.api_key = api_key
        self.symbols = {'all': 'all'}

    def _pre_start(self, loop) -> None:
        if self.api_key is None:
            raise APIKeyRequired("Bloxroute API key required")