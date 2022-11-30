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
        self.block_sub_id = await conn.make_call('eth_subscribe', ['newHeads'])['result']
        self.logs_sub_id = await conn.make_call('eth_subscribe', ['logs', {'topics': []}])['result']

    # TODO: Implement this with the Avro Kafka Connector
    @classmethod
    def serialize(cls, msg, schema_map):
        pass

    def get_transactions_by_block(self, conn: RPC, block_number: int):
        res = conn.make_call('eth_getBlockByNumber', [hex(block_number), True])
        return res['result']['transactions']

    # TODO: Implement these to send data to Kafka after preprocessing
    async def _transactions(self, conn: RPC, transactions: list):
        pass

    async def _log(self, conn: RPC, log: dict):
        pass

    async def _block(self, conn: RPC, block: dict):
        pass

    async def _token_transfer(self, conn: RPC, transfer: dict):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        msg = json.loads(message)
        data = msg['params']
        if data['subscription'] == self.block_sub_id:
            block_number = int(data['result']['number'], 16)
            transactions = self.get_transactions_by_block(conn, block_number)
            await self._transactions(conn, transactions)
        else:
            await self._log(conn, data['result'])
            topics = data['result']['topics']
            if topics[0].casefold() == TRANSFER_TOPIC:
                await self._token_transfer(conn, data['result'])
            
