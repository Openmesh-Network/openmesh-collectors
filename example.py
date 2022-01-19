"""
example

Contains example of how to use my code scaffold.

TO THE JDT: Make sure you look at the ws_factories.py file and the normalising_strategies.py file.
            I've assigned functions for yall to write.
"""
from src.normaliser.normaliser import Normaliser
import src.normaliser.metrics as metrics

def main():
    normaliser = Normaliser("okex")
    metrics_list = [metrics.OrderBookImbalance(), metrics.MidPrice(), metrics.MicroPrice()]
    while True:
        try:
            normaliser.dump() # Print tables 
            print("\n\n-----------INDICATORS-----------\n") 
            [print("%s%.5f" % metric.calculate(normaliser)) for metric in metrics_list]
            print("\n------------------------------------\n\n")
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()