from configparser import ConfigParser
import socket
import errno
import json


class Client():
    BUF_SIZE = 1024

    def __init__(self):
        self._read_config()
        self._connect()

        self._consume_messages()
    
    def _consume_messages(self):
        while True:
            msg = self._receive()
            if msg:
                print(json.loads(msg, indent=4))

    def _receive(self):
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
    
    def _get_url(self):
        config = ConfigParser()
        config.read("config.ini")
        attr = config['SOCKET']
        self.address = attr['address']
        self.host = attr['host']

    def _connect(self):
        self.socket = socket.create_connection((self.address, self.host))
        self.socket.setblocking(False)