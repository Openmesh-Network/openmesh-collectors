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
    config = ConfigParser()
    config.read(config_path)
    return {
        **config['KAFKA'],
        **dotenv_values(env_path)
    }

def get_redis_config():
    config = ConfigParser()
    config.read(config_path)
    return {
            **config['REDIS'],
            **dotenv_values(env_path)
        }