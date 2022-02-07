import time
from confluent_kafka import Producer, KafkaError, KafkaException
import sys
from queue import Queue
import json

class NormalisedDataProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'ssl.certificate.location': '../../jay.cert',
            'ssl.key.location': '../../jay.key',
            'ssl.ca.location': '../../ca-aiven-cert.pem',
            'client.id': 'coinbase-normalised-producer',
        }
        self.producer = Producer(self.conf)
        print("Created producer for topic %s" % self.topic)

    def _ack(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (msg.topic(), msg.partition(), msg.key(), msg.value()))
        else:
            print("Produced message to %s [%d] @ offset %d" %
                  (msg.topic(), msg.partition(), msg.offset()))

    def produce(self, key, msg):
        try:
            self.producer.produce(self.topic, key=key, value=json.dumps(msg), on_delivery=self._ack)
            self.producer.poll(0)
        except:
            print("Queue is full; waiting")
            self.producer.poll(0)
            time.sleep(0.5)
            self.producer.produce(self.topic, key=key, value=json.dumps(msg), on_delivery=self._ack)