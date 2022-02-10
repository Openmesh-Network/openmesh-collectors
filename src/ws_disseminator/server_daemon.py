import websockets
import asyncio
import aioconsole
import ssl
import pathlib
from configparser import ConfigParser

from .client_handler import handle_ws
from . import relay


CONFIG_PATH = "config.ini"

async def start_server(arg_port=None):
    host, port, cert, key = _read_config()
    if arg_port:
        port = arg_port
    stop = asyncio.Future()

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(cert, key)

    server_task = asyncio.create_task(run_server(host, port, ssl_context, stop))
    stop_task = asyncio.create_task(stop_server(stop))
    debug_task = asyncio.create_task(debug(stop))
    await asyncio.gather(
        server_task, 
        stop_task, 
        debug_task
    )
    print("Server Shut Down")

async def run_server(host, port, ssl_context, stop):
    print("Server Listening")
    async with websockets.serve(handle_ws, host, port, ssl=ssl_context):
        await stop

async def stop_server(stop):
    print("Press Enter to exit")
    await aioconsole.ainput()
    print("Exiting...")
    stop.set_result(True)

async def debug(stop):
    while not stop.done():
        await relay.debug()
        await asyncio.sleep(1)

def _read_config():
    parser = ConfigParser()
    parser.read(CONFIG_PATH)

    conf = parser['SOCKET']
    host = conf['host']
    port = int(conf['port'])
    cert = conf['cert']
    key = conf['key']
    return host, port, cert, key