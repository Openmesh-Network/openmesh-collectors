import websockets
import asyncio
import aioconsole
import ssl
import pathlib
import signal
from configparser import ConfigParser

from .client_handler import handle_ws
from . import relay


CONFIG_PATH = "config.ini"

stop = None

async def start_server(arg_port=None):
    global stop

    host, port, cert, key = _read_config()
    if arg_port:
        port = arg_port
    
    stop = asyncio.Future()

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(cert, key)

    signal.signal(signal.SIGUSR1, stop_server)

    server_task = asyncio.create_task(run_server(host, port, ssl_context))
    # stop_task = asyncio.create_task(stop_server(stop))
    debug_task = asyncio.create_task(debug())
    await asyncio.gather(
        server_task, 
        # stop_task, 
        debug_task
    )
    print("Server Shut Down")

async def run_server(host, port, ssl_context):
    print("Server Listening")
    async with websockets.serve(handle_ws, host, port, ssl=ssl_context):
        await stop


def stop_server(signum, frame):
    print("Interrupt signal received.")
    stop.set_result(True)

async def debug():
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