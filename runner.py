import asyncio
import sys


def main():

    if len(sys.argv) > 1 and sys.argv[1] == 'connector':

        from l3_atom.off_chain import mapping

        exchange_feed = mapping[sys.argv[2]]()
        loop = asyncio.get_event_loop()
        exchange_feed.start(loop)

        loop.run_forever()
    elif len(sys.argv) > 1 and sys.argv[1] == 'processor':

        from l3_atom.stream_processing import app

        del sys.argv[1]

        sys.argv.extend(['worker', '-l', 'info'])
        f_app = app.init()
        f_app.main()

    else:
        print('''USAGE:
    python3 runner.py connector <exchange>
    python3 runner.py processor''')


if __name__ == "__main__":
    main()
