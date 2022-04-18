"""
example

Contains example of how to use my code scaffold.

TO THE JDT: Make sure you look at the ws_factories.py file and the normalising_strategies.py file.
            I've assigned functions for yall to write.
"""
from configparser import ConfigParser
import time
import json
import multiprocessing
import sys

from normaliser import Normaliser
import metrics


def start_normaliser(exchange: str, symbol: str):
    normaliser = Normaliser(exchange, symbol)
    metrics_list = [
        metrics.NumberOfLOBEvents(), 
        metrics.RatioOfLobEvents(), 
        metrics.MidPrice(), 
        metrics.MicroPrice(), 
        metrics.OrderBookImbalance()
    ]
    for metric in metrics_list:
        normaliser.add_metric(metric)

    while True:
        try:
            #normaliser.dump()  # Print tables
            time.sleep(1/60)
        except KeyboardInterrupt:
            break


processes = []

def main():
    # Exchanges defined in main config file config.ini
    
    config = ConfigParser()
    if len(sys.argv) > 1:
        exchange = sys.argv[1:]
    else:
        exchange = "dydx"

    # Ticker symbols specified in config files in the "symbols" directory
    # config_path = exchange + ".ini"
    config_path = "config.ini"
    config.read(config_path)
    symbols = json.loads(config["symbols"][exchange])
    for symbol in symbols:
        process_name = exchange + ":" + symbol
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
