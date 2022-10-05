import asyncio
from l3_atom.off_chain import Coinbase
import logging

def main():
    
    loop = asyncio.get_event_loop()
    coinbase_feed = Coinbase()
    coinbase_feed.start(loop)

    loop.run_forever()

if __name__ == "__main__":
    main()