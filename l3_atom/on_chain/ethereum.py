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

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection, HTTPRPC, WSRPC
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging

TRANSFER_TOPIC = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

class Ethereum(ChainFeed):
    name = "ethereum"
    data_types = ['blocks', 'transactions', 'logs', 'token_transfers']

    async def subscribe(self, conn, feeds, symbols):
        blocks_res = await conn.make_call('eth_subscribe', ['newHeads'])
        self.block_sub_id = blocks_res['result']
        logging.info(f"Subscribed to newHeads with id {self.block_sub_id}")
        logs_res = await conn.make_call('eth_subscribe', ['logs', {'topics': []}])
        self.log_sub_id = logs_res['result']
        logging.info(f"Subscribed to logs with id {self.log_sub_id}")

    # TODO: Implement this with the Avro Kafka Connector
    @classmethod
    def serialize(cls, msg, schema_map):
        pass

    async def get_transactions_by_block(self, conn: RPC, block_number: int):
        res = await conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        print(res)
        return res['result']['transactions']

    # TODO: Implement these to send data to Kafka after preprocessing
    async def _transactions(self, conn: RPC, transactions: list):
        logging.info(f"Received {len(transactions)} transactions")
        print(transactions[0])

    async def _log(self, conn: RPC, log: dict):
        logging.info(f"Received log")

    async def _block(self, conn: RPC, block: dict):
        logging.info(f"Received block")

    async def _token_transfer(self, conn: RPC, transfer: dict):
        logging.info(f"Received token transfer")
        print(transfer)

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        msg = json.loads(message)
        data = msg['params']
        print(data['subscription'])
        if data['subscription'] == self.block_sub_id:
            block_number = int(data['result']['number'], 16)
            transactions = await self.get_transactions_by_block(conn, block_number)
            await self._transactions(conn, transactions)
        else:
            await self._log(conn, data['result'])
            topics = data['result']['topics']
            if topics[0].casefold() == TRANSFER_TOPIC:
                await self._token_transfer(conn, data['result'])
            
