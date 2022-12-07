from yapic import json
import traceback

from hexbytes import HexBytes


class EthereumLogHandler:
    """
    Handler for Ethereum logs. The main Ethereum standardiser maps topics to these handlers to filter and standardise incoming logs. Each handler monitors a specific topic with a unique contract address and ABI.

    @param topic0: the topic to filter on
    @param graph_endpoint: the graph endpoint to query for additional data if necessary. Typically changes on a per-protocol basis.
    @param abi_path: the path to the ABI file for the contract
    @param example_contract: an example contract address to use for parsing logs
    @param standardiser: reference to the parent standardiser
    """
    topic0: str = NotImplemented
    event_name: str = NotImplemented
    graph_endpoint: str = NotImplemented
    abi_name: str = NotImplemented
    example_contract: str = NotImplemented

    def __init__(self, standardiser) -> None:
        self.standardiser = standardiser
        self.web3 = standardiser.web3
        self.contract = self.web3.eth.contract(abi=json.loads(
            open(f'static/abis/{self.abi_name}.json').read()), address=self.example_contract)

    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        """Callback for after an event is processed"""
        pass

    async def process_log(self, log) -> None:
        """Process a log"""
        try:
            log = log.asdict()
            log['topics'] = [HexBytes(t) for t in [
                log['topic0'], log['topic1'], log['topic2'], log['topic3']] if t]
            event = self.contract.events[self.event_name]().processLog(log)
        except Exception:
            traceback.print_exc()
        await self.event_callback(event, blockTimestamp=log['blockTimestamp'], atomTimestamp=log['atomTimestamp'])
