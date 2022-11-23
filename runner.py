import asyncio
import sys
import argparse

from l3_atom.off_chain import mapping

def run_connector(source, symbol):
    connector = mapping[source](symbols=[symbol])

    loop = asyncio.get_event_loop()
    connector.start(loop)

    loop.run_forever()

def run_processor():
    from l3_atom.stream_processing import app

    old_args = sys.argv[2:]
    sys.argv = [sys.argv[0]]

    sys.argv.extend(['worker', '-l', 'info'])
    sys.argv.extend(old_args)
    f_app = app.init()
    f_app.main()

def main():

    parser = argparse.ArgumentParser(description='Run an L3 Atom process. Either a raw data consumer or a normalised data stream processor.')

    subparser = parser.add_subparsers(dest='function', help='The function to run', required=True)

    connector_parser = subparser.add_parser('connector', help='Run a raw data consumer')
    connector_parser.add_argument('--source', '-c', choices=mapping.keys(), help='The data source to connect to', required=True)
    connector_parser.add_argument('--symbol', '-s', help='The trading symbol to subscribe to', required=True)

    subparser.add_parser('processor', help='Run a normalised data stream processor')

    args = parser.parse_args()
    if args.function == 'connector':
        run_connector(args.source, args.symbol)
    elif args.function == 'processor':
        run_processor()


if __name__ == "__main__":
    main()
