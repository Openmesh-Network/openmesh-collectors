from l3_atom.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler

class UniswapV3Handler(EthereumLogHandler):
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'

class UniswapV3SwapHandler(UniswapV3Handler):
    topic0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    event_name = "Swap"
    abi_name = 'uniswap_v3_pool'
    example_contract = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'

    async def event_callback(self, event):
        block_num = event.blockNumber
        block_hash = event.blockHash
        tx_hash = event.transactionHash
        log_index = event.logIndex