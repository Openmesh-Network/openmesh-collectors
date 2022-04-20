from confluent_kafka import Producer, KafkaError, KafkaException
import asyncio
import sys
import json

class KafkaProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'ssl.certificate.location': 'jay.cert',
            'ssl.key.location': 'jay.key',
            'ssl.ca.location': 'ca-aiven-cert.pem',
            'client.id': f'{topic}-producer',
            'queue.buffering.max.messages': 1000000,
        }
        self.producer = Producer(self.conf)

    def _ack(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (msg.topic(), msg.partition(), msg.key(), msg.value()), file = sys.stderr)

    async def produce(self, key, msg):
        if isinstance(msg, bytes):
            msg = json.loads(msg)
        produced = False
        while not produced:
            try:
                produced = True
                self.producer.produce(self.topic, key=key, value=json.dumps(msg), on_delivery=self._ack)
                self.producer.poll(0)
            except BufferError as e:
                print(e)
                self.producer.flush()
                print("Queue flushed")
                produced = False