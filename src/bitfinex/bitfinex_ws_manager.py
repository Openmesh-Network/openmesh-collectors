import json
import time
from threading import Thread, Lock, Event
from queue import Queue
from typing import Callable
from gzip import decompress
from websocket import WebSocketApp
from confluent_kafka import Producer

class BitfinexWebsocketManager():
    _CONNECT_TIMEOUT_S = 5

    def __init__(self, symbol: str):
        """
        subscribe is a function that's called right after the websocket connects.
        unsubscribe is a function that's called just before the websocket disconnects.

        both subscribe and unsubscribe MUST have one argument, which is an instance of 
        WebsocketManager (see KrakenWsManagerFactory in ws_factories.py for an example).
        """
        self.connect_lock = Lock()
        self.ws = None
        self.temp_queue = Queue()
        self.queue = Queue()
        self.url = "wss://api-pub.bitfinex.c2"
        self.symbol = symbol
        self.subscribed = Event()
        self.subscribed.clear()
        self.connect()
        conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'client.id': 'bitfinex-python-producer',
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
            print("Produced record to topic {} partition [{}] @ offset {} with key {}"
                  .format(msg.topic(), msg.partition(), msg.offset(), msg.key()))

    def get_msg(self):
        """
        Retrieves a message from the front of the queue.

        NOTE: The message received has an extra field "received_timestamp", which
              is the UTC timestamp of when the message was received in milliseconds.
        """
        if self.temp_queue.qsize() > 0:
            return self.temp_queue.get()
        return self.queue.get()

    def _on_message(self, ws, message):
        message = json.loads(message)
        print(message)
        if isinstance(message, dict):
            message["receive_timestamp"] = int(time.time()*10**3)
        elif isinstance(message, list):
            message.append(int(time.time()*10**3))
            if self.subscribed.is_set():
                try:
                    symbol = self.symbol[1:]
                    #print(symbol)
                    #self.producer.send("quickstart", message)
                    #self.producer.produce('BTC-USD', message)
                    #print(message)
                    self.producer.produce(f"test-bitfinex-raw", key="%s:%s" % ("Bitfinex", self.url), value=json.dumps(message), on_delivery=self._acked)
                    self.producer.poll(0)
                except Exception as e:
                    print("Error producing message: %s" % e)
        else:
            raise TypeError(f"unrecognised message type {type(message)}")
        if not self.subscribed.is_set():
            self.temp_queue.put(message)
    
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
            on_error=self._wrap_callback(self._on_error),
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
                    f"Failed to connect to websocket url {self.url}")
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
            print("Already connected")
            return
        with self.connect_lock:
            while not self.ws:
                self._connect()
                if self.ws:
                    self.subscribe()
                    print("Subscribed")
                    return
    
    def subscribe(self):
        request = {
            "event": "subscribe",
            "channel": "book",
            "symbol": self.symbol,
            "prec": "R0",
            "len": "250",
        }
        self.send_json(request)

        msg = self.temp_queue.get()
        self.book_channel_id = None
        while self.book_channel_id == None:
            if isinstance(msg, dict) and "channel" in msg.keys() and msg["channel"] == "book":
                self.book_channel_id = msg["chanId"]
            else:
                if isinstance(msg, dict):
                    self.temp_queue.put(msg) # In case the trades feed subscribes first
                else:
                    self.queue.put(msg)
                msg = self.temp_queue.get()

        del request["len"]
        request["channel"] = "trades"
        self.send_json(request)

        msg = self.temp_queue.get()
        self.trades_channel_id = None
        while not self.trades_channel_id:
            if isinstance(msg, dict) and "channel" in msg.keys() and msg["channel"] == "trades":
                self.trades_channel_id = msg["chanId"]
            else:
                self.queue.put(msg)
                msg = self.temp_queue.get()
        
        self.subscribed.set()
    
    def unsubscribe(self):
        request = {
            "event": "unsubscribe",
            "chanId": self.book_channel_id
        }
        self.send_json(request)

        request["chanId"] = self.trades_channel_id
        self.send_json(request)
    
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

def main():
    ws = BitfinexWebsocketManager(symbol="tBTCUSD")
    ws.connect()
    ws.subscribed.wait()
    while True:
        pass

if __name__ == "__main__":
    main()