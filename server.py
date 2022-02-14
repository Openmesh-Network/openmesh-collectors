from src.ws_disseminator.server_daemon import start_server
import asyncio
import sys

if __name__ == "__main__":
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    else:
        port = None
    asyncio.run(start_server(port), debug=False)