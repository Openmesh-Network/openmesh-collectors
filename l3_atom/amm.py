from abc import abstractmethod
from typing import Union
from l3_atom.helpers.read_config import get_conf_symbols, get_node_provider
from l3_atom.helpers.enrich_data import enrich_raw
from datetime import datetime as dt
import asyncio
import requests
import uvloop
from yapic import json
import web3
from l3_atom.chain import ChainFeed

from l3_atom.feed import AsyncConnectionManager, AsyncFeed, WSConnection
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector

import logging

class AMM:
    abis: dict = NotImplemented


    
class AMMFeed(ChainFeed, AMM):
    pass