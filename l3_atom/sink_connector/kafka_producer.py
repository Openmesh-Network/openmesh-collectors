from confluent_kafka import Producer, KafkaError, KafkaException
from helpers.read_config import get_kafka_config
import asyncio
import sys
import json

class KafkaProducer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = get_kafka_config()
        self.conf['client.id'] = topic + '-producer'
        self.conf['queue.buffering.max.messages'] = 1000000
        self.producer = Producer(self.conf)

    def _ack(self, err, msg):
        if err is not None:
            print("Failed to deliver message: %s: %s" % (msg.topic(), msg.partition()))

    def produce(self, key, msg):
        if isinstance(msg, dict) or isinstance(msg, list):
            msg = json.dumps(msg).encode('utf-8')
        produced = False
        while not produced:
            try:
                produced = True
                self.producer.produce(self.topic, key=key, value=msg, on_delivery=self._ack, partition=0)
                self.producer.poll(0)
            except BufferError as e:
                print(e)
                self.producer.flush()
                print("Queue flushed")
                produced = False