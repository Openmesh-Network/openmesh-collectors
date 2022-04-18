from configparser import ConfigParser
import json

config_path = "config.ini"

def get_symbols(exchange: str):
    config = ConfigParser()
    config.read(config_path)
    return json.loads(config['symbols'][exchange])