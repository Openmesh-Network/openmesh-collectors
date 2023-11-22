from openmesh.stream_processing.standardiser import Standardiser
from openmesh.on_chain import Ethereum
import aiohttp
import web3
from .log_handlers import log_handlers


class EthereumStandardiser(Standardiser):
    raw_topic = 'ethereum_logs'
    exchange = Ethereum

    def __init__(self) -> None:
        super().__init__()
        self.feeds = ['dex_trades', 'dex_liquidity']
        self.normalised_topics = {
            feed: None for feed in self.feeds
        }
        self.graph_connected = False

    def start_exchange(self):
        # Bandaid solution to the race condition
        if isinstance(self.exchange, Ethereum):
            return
        self.exchange = self.exchange()
        self.web3 = web3.Web3(web3.Web3.HTTPProvider(self.exchange.node_conf['node_http_url']))
        self.web3.middleware_onion.add(web3.middleware.attrdict_middleware)
        self.log_handlers = {
            handler.topic0: handler(self) for handler in log_handlers
        }
        self.exchange_started = True

    async def _open_graph_conn(self):
        self.graph_conn = aiohttp.ClientSession()
        self.graph_connected = True

    async def graph_query(self, query: str) -> dict:
        """Query the graph endpoint"""
        if not self.graph_connected:
            await self._open_graph_conn()
        async with self.graph_conn.post(self.graph_endpoint, json={'query': query}) as resp:
            return await resp.json()

    async def handle_message(self, msg):
        """Handle a message from the raw topic"""
        if msg.topic0 in self.log_handlers:
            await self.log_handlers[msg.topic0].process_log(msg)
        else:
            pass
