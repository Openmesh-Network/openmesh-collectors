"""
example

Contains example of how to use my code scaffold.

TO THE JDT: Make sure you look at the ws_factories.py file and the normalising_strategies.py file.
            I've assigned functions for yall to write.
"""
from src.normaliser.normaliser import Normaliser

def main():
    normaliser = Normaliser("okex")
    while True:
        try:
            normaliser.dump() # Print tables 
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()