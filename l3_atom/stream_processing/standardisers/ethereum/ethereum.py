from l3_atom.stream_processing.standardiser import Standardiser
from l3_atom.on_chain import Ethereum
import aiohttp
import web3
from .log_handlers import log_handlers

class EthereumStandardiser(Standardiser):
    raw_topic = 'ethereum_logs'
    exchange = Ethereum

    def __init__(self) -> None:
        super().__init__()
        self.start_exchange()
        self.web3_endpoint = self.exchange.node_conf['node_http_url']
        self.web3 = web3.Web3(web3.HTTPProvider(self.web3_endpoint))
        self.log_handlers = {
            handler.topic0: handler(self) for handler in log_handlers
        }
        self.feeds = ['dex_trades']
        self.normalised_topics = {
            feed: None for feed in self.feeds
        }
        self.graph_connected = False

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
