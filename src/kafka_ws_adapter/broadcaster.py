from confluent_kafka import Consumer, KafkaError, KafkaException
from configparser import ConfigParser
from select import select
import socket
import threading
import sys
import json
import time


class KafkaWebsocketServer():
    def __init__(self):
        # self.client_threads = []
        self.connections = []
        self.connections_lock = threading.Lock()

        self._read_config()
        # self._setup_consumer()
        self._setup_ws_server()

        """
        self.broadcasting_thread = threading.Thread(
            name = "broadcaster",
            target = self._broadcast,
            daemon = True
        )
        self.broadcasting_thread.start()
        """

        self.connection_cleaner = threading.Thread(
            name = "connection_cleaner",
            target = self._clean_connections,
            daemon = True
        )
        self.connection_cleaner.start()

        self.connection_receiver = threading.Thread(
            name = "connection_receiver",
            target = self._receive_connections,
            daemon = True
        )
        self.connection_receiver.start()

        input()

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
        """
        Broadcasts message to all websocket listeners.

        For simplicity, currently loops through all connections and posts a message.
        """
        self.connections_lock.acquire()
        for subscriber in self.connections:
            subscriber.sendall(json.dumps(msg).encode('utf-8'))
        self.connections_lock.release()
    
    def _handle_client_connection(self, client_socket, address):
        """To be implemented for simultaneous broadcasting"""
        pass
    
    def _clean_connections(self):
        """Remove connections which are closed from sending list."""
        while True:
            self.connections_lock.acquire()
            if len(self.connections) == 0:
                self.connections_lock.release()
                continue

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
            try:
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
            except KeyboardInterrupt:
                break
        self.receiver.close()

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
        self.port = int(conf['port'])
        self.max_connections = int(conf['max.connections'])
    
    def _setup_consumer(self):
        """Consuming from Kafka Cluster"""
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
    
    def _setup_ws_server(self):
        self.receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.receiver.bind((self.address, self.port))
        self.receiver.listen(self.max_connections)


if __name__ == "__main__":
    server = KafkaWebsocketServer()
    while len(server.connections) != 1:
        1+1

    with open("test_data.json", "r") as f:
        data = f.read()
        server._send_to_connections(data)