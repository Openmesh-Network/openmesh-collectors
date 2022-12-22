from configparser import ConfigParser
import json
from dotenv import dotenv_values

config_path = "config.ini"
env_path = "keys/.env"


def get_conf_symbols(exchange: str):
    config = ConfigParser()
    config.read(config_path)
    return json.loads(config['SYMBOLS'][exchange])


def get_kafka_config():
    return dotenv_values(env_path)


def get_ethereum_provider():
    secrets = dotenv_values(env_path)
    return {
        "eth_node_ws_url": secrets["ETHEREUM_NODE_WS_URL"],
        "eth_node_http_url": secrets["ETHEREUM_NODE_HTTP_URL"],
        "eth_node_secret": secrets["ETHEREUM_NODE_SECRET"]
    }


def get_secrets():
    return dotenv_values(env_path)


def get_redis_config():
    config = ConfigParser()
    config.read(config_path)
    return {
        **config['REDIS'],
        **dotenv_values(env_path)
    }
