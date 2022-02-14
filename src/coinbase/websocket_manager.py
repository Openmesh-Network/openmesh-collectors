import json
import time
import asyncio
import requests
from threading import Thread, Lock
from queue import Queue
from typing import Callable
from gzip import decompress
from urllib import request
from websocket import WebSocketApp
from confluent_kafka import Producer

class WebsocketManager():
    _CONNECT_TIMEOUT_S = 5

    def __init__(self, url: str, subscribe: Callable, unsubscribe: Callable):
        """
        subscribe is a function that's called right after the websocket connects.
        unsubscribe is a function that's called just before the websocket disconnects.

        both subscribe and unsubscribe MUST have one argument, which is an instance of 
        WebsocketManager (see KrakenWsManagerFactory in ws_factories.py for an example).
        """
        self.connect_lock = Lock()
        self.ws = None
        self.queue = Queue()
        self.url = url
        self.subscribe = subscribe
        self.unsubscribe = unsubscribe
        self.connect()
        conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'client.id': 'kafka-python-producer',
            'ssl.certificate.location': 'jay.cert',
            'ssl.key.location': 'jay.key',
            'ssl.ca.location': 'ca-aiven-cert.pem',
        }
        self.producer = Producer(conf)
        
    def _acked(self, err, msg):
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            #delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    def get_msg(self):
        """
        Retrieves a message from the front of the queue.

        NOTE: The message received has an extra field "received_timestamp", which
              is the UTC timestamp of when the message was received in milliseconds.
        """
        return self.queue.get()

    def _on_message(self, ws, message):
        if isinstance(message, bytes) and "huobi" in self.url:
            message = json.loads(decompress(message))
        else:
            message = json.loads(message)
        if isinstance(message, dict):
            message["receive_timestamp"] = int(time.time()*10**3)
            try:
                symbol = message["product_id"].replace("-", "")
                #print(symbol)
                #self.producer.send("quickstart", message)
                #self.producer.produce('BTC-USD', message)
                self.producer.produce("test-coinbase-raw", key="%s:%s" % ("Coinbase", self.url), value=json.dumps(message), on_delivery=self._acked)
                #print(f"Produced {symbol}")
                self.producer.poll(0)
            except:
                pass
    
    def get_q_size(self):
        """Returns the size of the queue"""
        print(f"Queue Backlog: {self.queue.qsize()}")

    def send(self, message):
        """Sends a message over the websocket"""
        self.connect()
        self.ws.send(message)

    def send_json(self, message):
        """Sends a json message over the websocket"""
        self.send(json.dumps(message))

    def _connect(self):
        """Creates a websocket app and connects"""
        assert not self.ws, "ws should be closed before attempting to connect"

        self.ws = WebSocketApp(
            self.url,
            on_message=self._wrap_callback(self._on_message),
            on_close=self._wrap_callback(self._on_close),
            on_error=self._wrap_callback(self._on_error)
        )

        wst = Thread(target=self._run_websocket, args=(self.ws,))
        wst.daemon = True
        wst.start()

        # Wait for socket to connect
        ts = time.time()
        while self.ws and (not self.ws.sock or not self.ws.sock.connected):
            if time.time() - ts > self._CONNECT_TIMEOUT_S:
                self.ws = None
                raise Exception(
                    f"Failed to connect to websocket url {self._get_url()}")
            time.sleep(0.1)

    def _wrap_callback(self, f):
        """Wrap websocket callback"""
        def wrapped_f(ws, *args, **kwargs):
            if ws is self.ws:
                try:
                    f(ws, *args, **kwargs)
                except Exception as e:
                    raise Exception(f'Error running websocket callback: {e}')
        return wrapped_f

    def _run_websocket(self, ws):
        """"Runs the websocket app"""
        try:
            ws.run_forever(ping_interval=30)
        except Exception as e:
            raise Exception(f'Unexpected error while running websocket: {e}')
        finally:
            pass
            # self._reconnect(ws)

    def _get_url(self):
        return self.url

    def _reconnect(self, ws):
        """Closes a connection and attempts to reconnect"""
        assert ws is not None, '_reconnect should only be called with an existing ws'
        if ws is self.ws:
            self.ws = None
            ws.close()
            self.connect()

    def connect(self):
        """Connects to the websocket"""
        if self.ws:
            return
        with self.connect_lock:
            while not self.ws:
                self._connect()
                if self.ws:
                    self.subscribe(self)
                    return
    
    def resubscribe(self):
        self.unsubscribe()
        self.subscribe()

    def _on_close(self, ws):
        print("Connection Closed")
        self.unsubscribe(self)
        self._reconnect(ws)

    def _on_error(self, ws, error):
        print(f"websocket error: {error}")
        self._reconnect(ws)

    def reconnect(self) -> None:
        if self.ws is not None:
            self._reconnect(self.ws)
