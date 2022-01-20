"""
example

Contains example of how to use my code scaffold.

TO THE JDT: Make sure you look at the ws_factories.py file and the normalising_strategies.py file.
            I've assigned functions for yall to write.
"""
from time import sleep
from configparser import ConfigParser
import json
import multiprocessing

from src.normaliser.normaliser import Normaliser
import src.normaliser.metrics as metrics


def start_normaliser(exchange: str, symbol: str):
    normaliser = Normaliser(exchange, symbol)
    metrics_list = [metrics.NumberOfLOBEvents(), metrics.RatioOfLobEvents(), metrics.MidPrice(), metrics.MicroPrice(), metrics.OrderBookImbalance()]
    for metric in metrics_list:
        normaliser.add_metric(metric)

    while True:
        try:
            normaliser.dump()  # Print tables
            sleep(1)
        except KeyboardInterrupt:
            break


processes = []

def main():
    exchanges = ["phemex"]

    for exchange in exchanges:
        config_path = "src/normaliser/manager/symbol_configs/" + exchange + ".ini"

        config = ConfigParser()
        config.read(config_path)
        symbols = json.loads(config["DEFAULT"]["symbols"])
        for symbol in symbols:
            process_name = exchange + "_" + symbol
            process = multiprocessing.Process(
                target = start_normaliser,
                name = process_name,
                args = (exchange, symbol),
                daemon = True
            )
            processes.append(process)
            process.start()
    
    while True:
        try:
            1+1 # Do nothing
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()
