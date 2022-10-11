import asyncio
import sys
import logging

def main():

    if sys.argv[1] == 'connector':

        from l3_atom.off_chain import mapping

        exchange_feed = mapping[sys.argv[2]]()
        loop = asyncio.get_event_loop()
        exchange_feed.start(loop)

        loop.run_forever()
    elif sys.argv[1] == 'processor':

        from l3_atom.stream_processing import app

        del sys.argv[1]
        
        sys.argv.extend(['worker', '-l', 'info'])
        f_app = app.run()
        f_app.main()

if __name__ == "__main__":
    main()