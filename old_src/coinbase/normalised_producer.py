from confluent_kafka import Producer, KafkaError, KafkaException
import sys
from queue import Queue
import json
import time

class NormalisedDataProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'ssl.certificate.location': 'jay.cert',
            'ssl.key.location': 'jay.key',
            'ssl.ca.location': 'ca-aiven-cert.pem',
            'client.id': 'coinbase-normalised-producer',
            'queue.buffering.max.messages': 500000, # is this too small?
            'queue.buffering.max.ms': 60000, # is this too long?
            'batch.num.messages': 100, # is this too small?
            'linger.ms': 100
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
        msg['exchange'] = "coinbase"
        msg['topic'] = self.topic
        try:
            self.producer.produce(self.topic, key=key, value=json.dumps(msg), on_delivery=self._ack)
            self.producer.poll(0)
        except BufferError:
            print("Queue is full; waiting")
            time.sleep(0.1)
            self.producer.poll(0.1)
            self.producer.produce(self.topic, key=key, value=json.dumps(msg), on_delivery=self._ack)

def main():
    conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'client.id': 'kafka-python-consumer'}
    consumer = Producer(conf)

    consumer.subscribe(["BTC-USD"])


if __name__ == "__main__":
    main()