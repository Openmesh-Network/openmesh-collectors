"""
example

Contains example of how to use my code scaffold.

TO THE JDT: Make sure you look at the ws_factories.py file and the normalising_strategies.py file.
            I've assigned functions for yall to write.
"""
from src.normaliser.normaliser import Normaliser
import src.normaliser.metrics as metrics
from time import sleep
from sys import executable


def main():
    normaliser = Normaliser("okex")
    metrics_list = [metrics.NumberOfLOBEvents(), metrics.RatioOfLobEvents(), metrics.MidPrice(), metrics.MicroPrice(), metrics.OrderBookImbalance()]

    while True:
        try:
            normaliser.dump()  # Print tables
            print("\n\n-----------METRICS-----------\n")
            
            try:
                [print(metric.display_metric(normaliser)) for metric in metrics_list]
            except Exception as e:
                 print("Insufficient data to calculate metrics")
                 print(e)
            print("\n------------------------------------\n\n")
            sleep(1)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
