from confluent_kafka import Consumer, KafkaError, KafkaException
from configparser import ConfigParser
from select import select
import socket
import threading
import sys
import json


class KafkaWebsocketServer():
    def __init__(self):
        # self.client_threads = []
        self.connections = []
        self.connections_lock = threading.Lock()

        self._read_config()
        self._setup_consumer()
        self._setup_ws_server()

        self.broadcasting_thread = threading.Thread(
            name = "broadcaster",
            target = self._broadcast,
            daemon = True
        )
        self.broadcasting_thread.start()

        self.connection_cleaner = threading.Thread(
            name = "connection_cleaner",
            target = self._clean_connections,
            daemon = True
        )
        self.connection_cleaner.start()

        self._receive_connections()
    
    def _broadcast(self):
        while True:
            try:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None: 
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    self._send_to_connections(msg)
            finally:
                self.consumer.close()
    
    def _send_to_connections(self, msg):
        """For simplicity, currently loops through all connections and posts a message."""
        self.connections_lock.acquire()
        for subscriber in self.connections:
            subscriber.sendall(json.dumps(msg).encode('utf-8'))
        self.connections_lock.release()
    
    def _handle_client_connection(self, client_socket, address):
        """To implement when simultaneous broadcasting is implemented"""
        pass
    
    def _clean_connections(self):
        """Remove connections which are closed from sending list."""
        while True:
            self.connections_lock.acquire()
            readable, writable, exceptional = select([], self.connections, self.connections)
            for dead_connection in exceptional:
                dead_connection.close()
                self.connections.remove(dead_connection)
            self.connections_lock.release()
    
    def _receive_connections(self):
        """
        Master thread for starting client handling threads.

        In future, implement something which allows messages to be broadcasted to every
        connection simultaneously.

        """
        while True:
            (client_socket, address) = self.receiver.accept()
            self.connections_lock.acquire()
            self.connections.append(client_socket)
            self.connections_lock.release()
            """
            thread = threading.Thread(
                name = "client_connection_" + str(len(self.client_threads)),
                target = self._handle_client_connection,
                args = (client_socket, address),
                daemon = True
            )
            thread.start()
            self.client_threads.append(thread)
            """

    def _read_config(self):
        parser = ConfigParser()
        parser.read("config.ini")

        conf = parser['CONSUMER']
        self.conf = {
            'bootstrap.servers': conf['bootstrap.servers'],
            'group.id': conf['group.id'],
            'client.id': conf['client.id'],
        }
        self.topic = conf['topic']

        conf = parser['SOCKET']
        self.address = conf['address']
        self.port = conf['port']
        self.max_connections = conf['max.connections']
    
    def _setup_consumer(self):
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
    
    def _setup_ws_server(self):
        self.receiver = socket.socket(socket.AF_INET, socket.SOCKET_STREAM)
        self.receiver.bind((self.address, self.port))
        self.receiver.listen(self.max_connections)