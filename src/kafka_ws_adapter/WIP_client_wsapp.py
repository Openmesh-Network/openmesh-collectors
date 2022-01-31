from configparser import ConfigParser
from websocket import WebSocketApp
import json
import threading


class Client():
    BUF_SIZE = 1024

    def __init__(self):
        self._read_config()
        self._connect()

        self._consume_messages()

        input()
        self.ws.close()
    

    def _consume_messages(self):
        wst = threading.Thread(
            target=self._run_websocket, 
            args=(self.ws,),
            daemon = True
        )
        wst.start()

    def _run_websocket(self, ws):
        """"Runs the websocket app"""
        try:
            ws.run_forever(ping_interval=30)
        except Exception as e:
            raise Exception(f'Unexpected error while running websocket: {e}')
        finally:
            pass

    def _receive(self):
        """
        try:
            buf = bytearray(self.socket.recv(self.BUF_SIZE))
            while data != b'':
                data = self.socket.recv(self.BUF_SIZE)
                if data != b'':
                    buf.extend(data)
        except socket.error as e:
            err = e.args[0]
            if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
                data = buf.decode('utf-8')
                return json.loads(data)
            else:
                return None
        """
    
    def _read_config(self):
        config = ConfigParser()
        config.read("config.ini")
        attr = config['SOCKET']
        self.address = attr['address']
        self.port = int(attr['port'])
        self.url = "ws://" + self.address + ":" + str(self.port) + "/"

    def _connect(self):
        self.ws = WebSocketApp(
            self.url,
            on_open=self._wrap_callback(self._on_open),
            on_message=self._wrap_callback(self._on_message),
            on_close=self._wrap_callback(self._on_close),
            on_error=self._wrap_callback(self._on_error)
        )
    
    def _on_open(self, ws):
        return

    def _on_message(self, msg):
        print("Message Received")
        print(json.loads(msg, indent=4))
    
    def _on_close(self, ws, close_status_code, close_msg):
        print(f"{close_status_code}: {close_msg}")

    def _on_error(self, ws, err):
        print(err)

    def _wrap_callback(self, f):
        """Wrap websocket callback"""
        def wrapped_f(ws, *args, **kwargs):
            if ws is self.ws:
                try:
                    f(ws, *args, **kwargs)
                except Exception as e:
                    raise Exception(f'Error running websocket callback: {e}')
        return wrapped_f


if __name__ == "__main__":
    Client()