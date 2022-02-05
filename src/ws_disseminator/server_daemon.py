import websockets
import asyncio
import aioconsole
from configparser import ConfigParser

from .client_handler import handle_ws
from . import relay


CONFIG_PATH = "config.ini"

async def start_server(arg_port=None):
    address, port = _read_config()
    if arg_port:
        port = arg_port
    stop = asyncio.Future()

    server_task = asyncio.create_task(run_server(address, port))
    stop_task = asyncio.create_task(stop_server())
    debug_task = asyncio.create_task(debug())
    done, pending = await asyncio.wait(
        [server_task, stop_task, debug_task],
        return_when = asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()

async def run_server(address, port):
    print("Server Listening")
    async with websockets.serve(handle_ws, address, port):
        await asyncio.Future()

async def stop_server():
    print("Press Enter to exit")
    await aioconsole.ainput()
    await relay.shutdown()
    print("Relay Shutdown")

async def debug():
    while True:
        await relay.debug()
        await asyncio.sleep(1)

def _read_config():
    parser = ConfigParser()
    parser.read(CONFIG_PATH)

    conf = parser['SOCKET']
    address = conf['address']
    port = int(conf['port'])
    return address, port