from configparser import ConfigParser
from websocket._core import create_connection
import socket
import json
import threading


class Client():
    BUF_SIZE = 4096

    def __init__(self):
        self._read_config()
        self._connect()

        self.consumer= threading.Thread(
            name = "consumer",
            target=self._consume_messages, 
            daemon = True
        )
        self.consumer.start()

        input()
        self.ws.close()
    
    def _consume_messages(self):
        """Main consumer thread"""
        while True:
            msg = self.ws.recv(self.BUF_SIZE)
            print(json.dumps(json.loads(msg), indent=4))

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
        self.ws = socket.create_connection((self.address, self.port))


if __name__ == "__main__":
    Client()