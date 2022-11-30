from abc import abstractmethod
from typing import Union
from l3_atom.helpers.read_config import get_conf_symbols, get_ethereum_provider
from l3_atom.helpers.enrich_data import enrich_raw
from datetime import datetime as dt
import asyncio
import requests
import uvloop
from yapic import json
import web3
from l3_atom.chain import ChainFeed

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection, HTTPRPC, WSRPC
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging

class Ethereum(ChainFeed):
    name = "ethereum"
    data_types = ['blocks', 'transactions', 'logs', 'token_transfers']

    async def subscribe(self, conn, feeds, symbols):
        self.block_sub_id = await conn.make_call('eth_subscribe', ['newHeads'])
        self.logs_sub_id = await conn.make_call('eth_subscribe', ['logs', {'topics': []}])

    @classmethod
    def serialize(cls, msg, schema_map):
        pass

    async def process_message(self, message: str, conn: AsyncFeed, timestamp: int):
        msg = json.loads(message)
