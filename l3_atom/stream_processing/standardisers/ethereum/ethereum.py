from l3_atom.stream_processing.standardiser import Standardiser
from l3_atom.on_chain import Ethereum
import aiohttp

class EthereumStandardiser(Standardiser):
    raw_topic = 'ethereum_logs'
    exchange = Ethereum

    # Endpoint to connect to The Graph
    graph_endpoint: str = NotImplemented

    def __init__(self) -> None:
        super().__init__()
        self.graph_conn = aiohttp.ClientSession()

    async def graph_query(self, query: str) -> dict:
        """Query the graph endpoint"""
        async with self.graph_conn.post(self.graph_endpoint, json={'query': query}) as resp:
            return await resp.json()