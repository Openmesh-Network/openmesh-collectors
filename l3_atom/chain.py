from abc import abstractmethod
from typing import Union
from l3_atom.helpers.read_config import get_secrets
from l3_atom.helpers.enrich_data import enrich_raw
from datetime import datetime as dt
import asyncio
import requests
import uvloop
from yapic import json
import web3
from l3_atom.data_source import AvroDataFeed
import base64

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection, HTTPRPC, WSRPC
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging


class Chain:

    def load_node_conf(self):
        secrets = get_secrets()
        return {k.split('_', maxsplit=1)[1].lower(): v for k, v in secrets.items() if k.startswith(self.name.upper())}


class ChainFeed(Chain, AvroDataFeed):

    node_conn: Union[HTTPRPC, WSRPC] = NotImplemented
    data_types: list = NotImplemented
    # { <feed>: <Kafka Producer> }
    kafka_backends: dict = NotImplemented

    def __init__(self):
        super().__init__(max_syms=None)
        self.node_conf = self.load_node_conf()
        self._init_node_conn(**self.node_conf)

        self.rpc_endpoints = {
            self.node_conf['node_ws_url']: [
                self.name
            ]
        }

    def _init_node_conn(self, http_node_url=None, node_secret=None, **kwargs):
        self.node_conn = HTTPRPC(http_node_url, auth_secret=node_secret)

    def _get_auth_header(self, username, password):
        assert ':' not in username
        auth = base64.b64encode(f'{username}:{password}'.encode()).decode()
        return ('Authorization', f'Basic {auth}')

    async def auth_ws(self, addr, options):
        return addr, {'extra_headers': [self._get_auth_header(username='', password=self.node_conf['node_secret'])]}

    def _init_kafka(self, loop: asyncio.AbstractEventLoop):
        logging.info('%s: Starting Kafka connectors', self.name)
        for feed in self.data_types:
            logging.info('%s: Starting Kafka connector for %s', self.name, feed)
            self.kafka_backends[feed] = KafkaConnector(
                self.name, feed, loop)
        self.kafka_backends.values()[0].create_chain_topics(self.data_types, self.name)
        for backend in self.kafka_backends.values():
            backend.start(loop)

    def start(self, loop: asyncio.AbstractEventLoop):
        """
        Start WS connections for raw chain data

        :param loop: Event loop to run the connection on
        :type loop: asyncio.AbstractEventLoop
        """
        # self._init_kafka(loop)
        rest_connections = self._init_rest()
        for connection in rest_connections:
            self.connection_handlers.append(AsyncConnectionManager(
                connection, None, self.process_message, None, None, self.retries, self.interval, self.timeout, self.delay))
        for (endpoint, channels) in self.rpc_endpoints.items():
            connection = WSRPC(
                self.name, addr=endpoint, authentication=self.auth_ws)
            self.connection_handlers.append(AsyncConnectionManager(
                connection, self.subscribe, self.process_message, None, channels, self.retries, self.interval, self.timeout, self.delay))

        for handler in self.connection_handlers:
            handler.start_connection(loop)

        logging.info('%s: Starting connection to %s chain',
                     self.name, self.name)
