from configparser import ConfigParser
import json

config_path = "config.ini"

def get_symbols(exchange: str):
    config = ConfigParser()
    config.read(config_path)
    return json.loads(config['SYMBOLS'][exchange])

def get_kafka_config():
    config = ConfigParser()
    config.read(config_path)
    return dict(config['PRODUCER'])