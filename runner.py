import asyncio
import sys
import argparse

from openmesh.off_chain import mapping as off_chain_mapping
from openmesh.on_chain import mapping as on_chain_mapping
import logging


def run_connector(source, symbol):
    connector = None

    if source in off_chain_mapping:
        connector = off_chain_mapping[source](symbols=[symbol])
    else:
        connector = on_chain_mapping[source]()

    loop = asyncio.get_event_loop()

    loop = asyncio.get_event_loop()
    connector.start(loop)

    loop.run_forever()


def set_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout)


def run_processor():
    from openmesh.stream_processing import app

    old_args = sys.argv[2:]
    sys.argv = [sys.argv[0]]

    sys.argv.extend(['worker', '-l', 'info'])
    sys.argv.extend(old_args)
    f_app = app.init()
    f_app.main()


def main():

    mapping = {**off_chain_mapping, **on_chain_mapping}

    logging_map = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }

    return

    parser = argparse.ArgumentParser(
        description='Run an Openmesh process. Either a raw data consumer or a normalised data stream processor.')

    parser.add_argument('--logging', '-l', default='info',
                        help='Set the logging level', choices=logging_map.keys())

    subparser = parser.add_subparsers(
        dest='function', help='The function to run', required=True)

    connector_parser = subparser.add_parser(
        'connector', help='Run a raw data consumer')
    connector_parser.add_argument(
        '--source', '-c', choices=mapping.keys(), help='The data source to connect to', required=True)
    connector_parser.add_argument(
        '--symbol', '-s', help='The trading symbol to subscribe to')

    subparser.add_parser(
        'processor', help='Run a normalised data stream processor')

    args = parser.parse_args()

    level = logging_map[args.logging] if args.logging else logging.INFO
    set_logging(level)

    if args.function == 'connector':
        run_connector(args.source, args.symbol)
    elif args.function == 'processor':
        run_processor()


if __name__ == "__main__":
    main()
