import asyncio
import sys
import argparse
import os
import threading
import time

from openmesh.off_chain import mapping as off_chain_mapping
from openmesh.on_chain import mapping as on_chain_mapping
from yapic import json
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


def run_measurer():
    # Load file with last measurements or create it if it doesn't exist
    # f = open('last.log', 'w')
    # f.close()

    # f = open('last.log', 'r')
    # lines = f.readlines()
    # f.close()

    # c_line = 0

    def get_data_for_chain(key):
        data_filename = 'data-' + key + '.csv'

        if not os.path.exists(data_filename):
            # Write header if the file hasn't been created yet
            f = open(data_filename, 'w')
            f.write('datasource' + ',' + 'symbol' + ',' + 'message_count' + ',' + 'bytes' + ',' + 'duration' + ',' + 'start_time' + ',' + 'end_time' + '\n')
            f.close()

        datasource = off_chain_mapping[key]()
        symbols = list(datasource.normalise_symbols(datasource.get_symbols()))

        synced = True # TODO: change to false and fix logging
        print(key, len(symbols))

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        i = 0

        while i < len(symbols):
            # if not synced or len(lines) <= c_line + 10:
            #     print('synced with last.log!')
            #     synced = True

            # elif str(symbols[i]) in lines[c_line]:
            #     # print('found in last.log')
            #     pass

            if synced:
                N_SYMBOLS = 150 # how many symbols are we requesting simultaneously
                SECONDS = 50
                start_time = int(time.time())
                end_time = 0

                # f = open('last.log', 'a')
                # for s in symbols[i:i+N_SYMBOLS]:
                #     f.write(str(s) + '\n')
                # f.close()

                # Now that we know all the data is new we can start tapping
                # into each exchange and record 
                stats_per_symbol = {}
                def mock_kafka(msg):
                    nonlocal stats_per_symbol
                    nonlocal datasource

                    key = datasource.get_sym_from_msg(json.loads(msg))

                    if key is not None:
                        if key not in stats_per_symbol:
                            stats_per_symbol[key] = []
                        stats_per_symbol[key].append(len(msg))

                symbol_count = min(N_SYMBOLS, len(symbols) - i)

                connector = off_chain_mapping[key](symbols=symbols[i:i+symbol_count],
                                                   mock_kafka=True,
                                                   mock_kafka_callback=mock_kafka)

                try:
                    loop = asyncio.get_event_loop()
                    connector.start(loop)
                finally:
                    loop.run_until_complete(asyncio.sleep(SECONDS))
                    connector.stop()

                end_time = int(time.time())

                # Save into csv
                f = open(data_filename, 'a')
                for symbol in stats_per_symbol:
                    arr = stats_per_symbol[symbol]
                    r_sum = 0
                    for j in arr:
                        r_sum += j

                    f.write(str(key) + ',' + str(symbol) + ',' + str(len(stats_per_symbol[symbol])) + ',' + str(r_sum) + ',' + str(SECONDS) + ',' + str(start_time) + ',' + str(end_time) + '\n')
                f.close()
                i += symbol_count

            else:
                c_line += 1
                # advance by 1
                i += 1


    threads = []
    for key in off_chain_mapping:
        if key == 'gemini' or key == 'opensea':
            continue

        # TODO: launch one thread per X number of exchanges, work out the rate limiting amount maybe?
        # Otherwise I could use a VPN or something like that

        threads.append(threading.Thread(target=get_data_for_chain, args=[key]))

    # should make this 8X faster
    for t in threads:
        t.start()
    for t in threads:
        t.join()


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

    parser = argparse.ArgumentParser(
        description='''Run an Openmesh process.
 Either a raw data consumer or a normalised data stream processor.''')

    parser.add_argument('--logging',
                        '-l',
                        default='info',
                        help='Set the logging level',
                        choices=logging_map.keys())

    subparser = parser.add_subparsers(
        dest='function', help='The function to run', required=True)

    connector_parser = subparser.add_parser(
        'connector', help='Run a raw data consumer')
    connector_parser.add_argument(
        '--source', '-c', choices=mapping.keys(),
        help='The data source to connect to', required=True)
    connector_parser.add_argument(
        '--symbol', '-s', help='The trading symbol to subscribe to')

    subparser.add_parser(
        'processor', help='Run a normalised data stream processor')

    measurer_parser = subparser.add_parser(
        'measurer', help='Measure average bandwidth')
    measurer_parser.add_argument(
        '--noresume', '-R', help='Don\'t resume from previous session')

    measurer_parser = subparser.add_parser(
        'measurer-loop', help='Run measurer in a loop to get more data')

    args = parser.parse_args()

    level = logging_map[args.logging] if args.logging else logging.INFO
    set_logging(level)

    if args.function == 'connector':
        run_connector(args.source, args.symbol)

    elif args.function == 'processor':
        run_processor()

    elif args.function == 'measurer':
        run_measurer()

    elif args.function == 'measurer-loop':
        while True:
            run_measurer()
            print('New measurer loop, yuppy!')

    return


if __name__ == "__main__":
    main()
