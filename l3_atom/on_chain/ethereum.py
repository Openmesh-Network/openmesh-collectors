from abc import abstractmethod
from typing import Union
from l3_atom.helpers.read_config import get_conf_symbols, get_ethereum_provider
from l3_atom.feed import RPC
from l3_atom.helpers.enrich_data import enrich_raw
from datetime import datetime as dt
import asyncio
import requests
import uvloop
from yapic import json
import web3
from l3_atom.chain import ChainFeed
import asyncio
import dataclasses

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection, HTTPRPC, WSRPC
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging
from decimal import Decimal

TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

# Handles the conversions between hex digits and their decimal equivalents automatically
class EthereumObject:
    def __post_init__(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.type is int and isinstance(value, str):
                setattr(self, field.name, int(value, 16))
            if field.type is Decimal and isinstance(value, str):
                setattr(self, field.name, Decimal(int(value, 16)))

    def to_dict(self):
        return dataclasses.asdict(self)

    def to_json_string(self):
        return json.dumps(self.to_dict())

@dataclasses.dataclass
class EthereumBlock(EthereumObject):
    feed = 'ethereum_blocks'
    baseFeePerGas: int
    number: int
    hash: str
    parentHash: str
    nonce: str
    sha3Uncles: str
    logsBloom: str
    transactionsRoot: str
    stateRoot: str
    receiptsRoot: str
    miner: str
    difficulty: Decimal
    extraData: str
    size: int
    gasLimit: int
    gasUsed: int
    timestamp: int

@dataclasses.dataclass
class EthereumTransaction(EthereumObject):
    feed = 'ethereum_transactions'
    blockTimestamp: int
    hash: str
    nonce: str
    blockHash: str
    blockNumber: int
    transactionIndex: int
    fromAddr: str
    toAddr: str
    value: Decimal
    gas: int
    gasPrice: int
    input: str
    maxFeePerGas: int
    maxPriorityFeePerGas: int
    type: int

@dataclasses.dataclass
class EthereumLog(EthereumObject):
    feed = 'ethereum_logs'
    blockTimestamp: int
    blockNumber: int
    blockHash: str
    transactionIndex: int
    transactionHash: str
    logIndex: int
    address: str
    data: str
    topic0: str
    topic1: str
    topic2: str
    topic3: str

@dataclasses.dataclass
class EthereumTransfer(EthereumObject):
    feed = 'ethereum_token_transfers'
    blockTimestamp: int
    blockNumber: int
    blockHash: str
    transactionHash: str
    logIndex: int
    fromAddr: str
    toAddr: str
    tokenAddr: str
    value: Decimal

class Ethereum(ChainFeed):
    name = "ethereum"
    data_types = ['blocks', 'transactions', 'logs', 'token_transfers']

    async def subscribe(self, conn, feeds, symbols):
        blocks_res = await conn.make_call('eth_subscribe', ['newHeads'])
        logging.debug(
            "%s: Attempting to subscribe to newHeads with result %s", self.name, blocks_res)
        self.block_sub_id = blocks_res['result']
        logging.info(f"Subscribed to newHeads with id {self.block_sub_id}")
        logs_res = await conn.make_call('eth_subscribe', ['logs', {'topics': []}])
        logging.debug(
            "%s: Attempting to subscribe to logs with result %s", self.name, logs_res)
        self.log_sub_id = logs_res['result']
        logging.info(f"Subscribed to logs with id {self.log_sub_id}")

    # TODO: Implement this with the Avro Kafka Connector
    @classmethod
    def serialize(cls, msg, schema_map):
        pass

    def hex_to_int(self, hex_str: str):
        return int(hex_str, 16)

    async def get_transactions_by_block(self, conn: RPC, block_number: int):
        res = await conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        while 'result' not in res:
            await asyncio.sleep(1)
            res = await conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        return res['result']['transactions']

    async def get_block_by_number(self, conn: RPC, block_number: str):
        res = await conn.make_call('eth_getBlockByNumber', [block_number, True])
        while 'result' not in res:
            await asyncio.sleep(1)
            res = await conn.make_call('eth_getBlockByNumber', [block_number, True])
        return res['result']

    # TODO: Implement these to send data to Kafka after preprocessing
    async def _transactions(self, conn: RPC, transactions: list):
        logging.debug(f"Received {len(transactions)} transactions")

    async def _log(self, conn: RPC, log: dict):
        logging.debug(f"Received log")

    async def _block(self, conn: RPC, block: dict):
        print("-----------------\n\n")
        logging.debug(f"Received block")
        print("\n\n-----------------")
        del block['mixHash']
        del block['transactions']
        del block['totalDifficulty']
        del block['uncles']
        print(block)
        blockObj = EthereumBlock(**block)
        print(blockObj)


    async def _token_transfer(self, conn: RPC, transfer: dict):
        logging.debug(f"Received token transfer")

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        msg = json.loads(message)
        data = msg['params']
        if data['subscription'] == self.block_sub_id:
            block_number = data['result']['number']
            block = await self.get_block_by_number(self.http_node_conn, block_number)
            await self._block(conn, block)
            await self._transactions(conn, block['transactions'])
        else:
            await self._log(conn, data['result'])
            topics = data['result']['topics']
            if topics[0].casefold() == TRANSFER_TOPIC:
                await self._token_transfer(conn, data['result'])
